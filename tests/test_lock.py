import asyncio
from unittest import IsolatedAsyncioTestCase

from fakeredis.aioredis import FakeRedis

from ddredis.lock import AlreadyAcquiredError, RedisLock

TIMEOUT = 10
TIMEOUT_MS = TIMEOUT * 1000


class TestRedisLock(IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.redis_client = FakeRedis()
        await self.redis_client.flushall()

    async def test_acquire_and_release(self):
        # Act
        async with RedisLock(self.redis_client, 'job', timeout=10) as lock:
            held = await self.redis_client.exists('job')

        # Assert
        self.assertIsInstance(lock, RedisLock)
        self.assertEqual(held, 1)
        self.assertEqual(await self.redis_client.exists('job'), 0)

    async def test_second_holder_is_rejected(self):
        # Arrange
        async with RedisLock(self.redis_client, 'job', timeout=10):
            # Act & Assert
            with self.assertRaises(AlreadyAcquiredError):
                async with RedisLock(self.redis_client, 'job', timeout=10):
                    pass

        # the first holder's release was not affected by the failed attempt
        self.assertEqual(await self.redis_client.exists('job'), 0)

    async def test_released_on_exception(self):
        # Act
        with self.assertRaises(ValueError):
            async with RedisLock(self.redis_client, 'job', timeout=10):
                raise ValueError('boom')

        # Assert
        self.assertEqual(await self.redis_client.exists('job'), 0)

    async def test_extend_replaces_ttl(self):
        # Arrange
        lock = RedisLock(self.redis_client, 'job', timeout=TIMEOUT)
        await lock.acquire()
        await self.redis_client.pexpire('job', TIMEOUT_MS // 10)  # pretend most of the TTL is gone

        # Act
        await lock.extend()
        ttl_ms = await self.redis_client.pttl('job')
        await lock.release()

        # Assert: back to the full timeout, not timeout + remaining
        self.assertTrue(TIMEOUT_MS * 0.9 < ttl_ms <= TIMEOUT_MS, ttl_ms)

    async def test_release_tolerates_an_expired_lock(self):
        # Arrange
        lock = RedisLock(self.redis_client, 'job', timeout=10)
        await lock.acquire()
        await self.redis_client.delete('job')  # Redis dropped the key as if the TTL had passed

        # Act & Assert: no LockNotOwnedError
        await lock.release()

    async def test_blocking_lock_waits_for_the_holder(self):
        # Arrange
        holder = RedisLock(self.redis_client, 'job', timeout=1)
        await holder.acquire()
        waiter = RedisLock(self.redis_client, 'job', timeout=1, blocking=True)

        async def release_soon():
            await asyncio.sleep(0.05)
            await holder.release()

        # Act
        await asyncio.gather(waiter.acquire(), release_soon())

        # Assert
        self.assertEqual(await self.redis_client.exists('job'), 1)
        await waiter.release()
