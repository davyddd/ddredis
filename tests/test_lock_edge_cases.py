from unittest import IsolatedAsyncioTestCase

from fakeredis.aioredis import FakeRedis
from redis.exceptions import LockError

from ddredis.lock import AlreadyAcquiredError, RedisLock

TIMEOUT = 10
TIMEOUT_MS = TIMEOUT * 1000


class TestRedisLockEdgeCases(IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.redis_client = FakeRedis()
        await self.redis_client.flushall()

    async def test_acquire_sets_the_ttl(self):
        # Act
        async with RedisLock(self.redis_client, 'job', timeout=TIMEOUT):
            ttl_ms = await self.redis_client.pttl('job')

        # Assert
        self.assertTrue(TIMEOUT_MS * 0.9 < ttl_ms <= TIMEOUT_MS, ttl_ms)

    async def test_aenter_returns_the_lock(self):
        # Act
        async with RedisLock(self.redis_client, 'job', timeout=TIMEOUT) as lock:
            # Assert
            self.assertIsInstance(lock, RedisLock)
            self.assertEqual(lock.timeout, TIMEOUT)

    async def test_release_of_a_never_acquired_lock_is_a_programming_error(self):
        # Arrange
        holder = RedisLock(self.redis_client, 'job', timeout=TIMEOUT)
        await holder.acquire()
        other = RedisLock(self.redis_client, 'job', timeout=TIMEOUT)

        # Act & Assert: redis-py refuses before talking to Redis; only an expired own lock is tolerated
        with self.assertRaises(LockError):
            await other.release()
        self.assertEqual(await self.redis_client.exists('job'), 1)
        await holder.release()
        self.assertEqual(await self.redis_client.exists('job'), 0)

    async def test_extend_without_holding_the_lock(self):
        # Arrange
        lock = RedisLock(self.redis_client, 'job', timeout=TIMEOUT)

        # Act & Assert: redis-py refuses to extend a lock this object never acquired
        with self.assertRaises(LockError):
            await lock.extend()

    async def test_blocking_lock_gives_up_after_timeout(self):
        # Arrange
        holder = RedisLock(self.redis_client, 'job', timeout=TIMEOUT)
        await holder.acquire()
        waiter = RedisLock(self.redis_client, 'job', timeout=0.2, blocking=True)

        # Act & Assert: waits up to its own timeout, then reports the lock as taken
        with self.assertRaises(AlreadyAcquiredError):
            await waiter.acquire()
        await holder.release()

    async def test_different_keys_do_not_interfere(self):
        # Act
        async with (
            RedisLock(self.redis_client, 'job-a', timeout=TIMEOUT),
            RedisLock(self.redis_client, 'job-b', timeout=TIMEOUT),
        ):
            # Assert
            self.assertEqual(await self.redis_client.exists('job-a', 'job-b'), 2)

    async def test_reacquire_after_release(self):
        # Arrange
        lock = RedisLock(self.redis_client, 'job', timeout=TIMEOUT)

        # Act & Assert
        await lock.acquire()
        await lock.release()
        await lock.acquire()
        self.assertEqual(await self.redis_client.exists('job'), 1)
        await lock.release()
