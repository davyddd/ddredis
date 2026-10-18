from collections.abc import Hashable
from typing import Any, Self

from ddutils.connections import ConnectionManagerFactory
from ddutils.context_helpers import async_suppress
from redis.asyncio import Redis
from redis.asyncio.lock import Lock, LockError, LockNotOwnedError


class AlreadyAcquiredError(Exception): ...


class RedisLock:
    """
    Distributed lock backed by Redis (`SET NX PX` + Lua release).

    The client is obtained from `client_factory`, a `ConnectionManagerFactory` called with `alias`, like
    `GenericCache`, inside every call: the lock borrows the client for one command and holds nothing of it
    between calls, only the token that identifies this holder. So the factory may hand out and release the
    client however its manager sees fit.

    Caveat: `timeout` is the Redis-side TTL of the key, not a client-side cancellation timer. If the protected
    code runs longer than `timeout`, Redis silently removes the key and a competing caller can acquire the same
    lock and execute in parallel, violating mutual exclusion. The original code keeps running unaware.
    `release()` swallows `LockNotOwnedError` for this case, so the cleanup is silent, but the invariant has
    already been broken upstream. Pick a `timeout` that comfortably exceeds the worst-case runtime of the block,
    or call `extend()` periodically (heartbeat) for long operations whose duration is hard to predict.
    """

    def __init__(
        self,
        client_factory: ConnectionManagerFactory[Any, Redis, Any],
        key: str,
        timeout: float = 10.0,
        blocking: bool = False,
        *,
        alias: Hashable | None = None,
    ):
        self.client_factory = client_factory
        self.alias = alias
        self.key = key
        self.timeout = timeout
        self.blocking = blocking
        self._token: bytes | str | None = None  # set while this object holds the lock

    def _lock(self, client: Redis) -> Lock:
        """redis-py's `Lock` on the borrowed client, carrying this holder's token when there is one."""
        lock = Lock(
            redis=client,
            name=self.key,
            timeout=self.timeout,
            blocking=self.blocking,
            blocking_timeout=self.timeout if self.blocking else None,
            thread_local=False,
        )
        if self._token is not None:
            lock.local.token = self._token
        return lock

    async def acquire(self) -> None:
        async with self.client_factory(alias=self.alias) as client:
            lock = self._lock(client)
            acquired = await lock.acquire()

        if not acquired:
            raise AlreadyAcquiredError('Lock is already acquired')
        self._token = lock.local.token

    async def release(self) -> None:
        if self._token is None:  # the same error redis-py raises for a lock this object does not hold
            raise LockError('Cannot release an unlocked lock')
        try:
            # LockNotOwnedError: the key expired and may be held by someone else now, nothing of ours to remove
            async with async_suppress(LockNotOwnedError), self.client_factory(alias=self.alias) as client:
                await self._lock(client).release()
        finally:
            self._token = None

    async def extend(self) -> None:
        if self._token is None:
            raise LockError('Cannot extend an unlocked lock')
        # `replace_ttl=True` because every caller means this as a heartbeat: the lock should
        # outlive the holder by `timeout`, counted from now. redis-py's default ADDS to the
        # remaining TTL, so a long-lived holder beating steadily would push the expiry further
        # out with every beat, and a lock whose holder is killed would then sit unreleased for
        # as long as it had been beating.
        async with self.client_factory(alias=self.alias) as client:
            await self._lock(client).extend(self.timeout, replace_ttl=True)

    async def __aenter__(self) -> Self:
        await self.acquire()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.release()


__all__ = ('AlreadyAcquiredError', 'RedisLock')
