from unittest import IsolatedAsyncioTestCase

from redis.exceptions import ConnectionError as RedisConnectionError, TimeoutError as RedisTimeoutError

from ddredis.decorators import suppress_redis_errors


class TestSuppressRedisErrors(IsolatedAsyncioTestCase):
    async def test_returns_the_value(self):
        # Arrange
        @suppress_redis_errors
        async def read() -> int:
            return 42

        # Act & Assert
        self.assertEqual(await read(), 42)

    async def test_redis_errors_become_none(self):
        # Arrange
        @suppress_redis_errors
        async def read(error: Exception) -> int:
            raise error

        # Act & Assert
        self.assertIsNone(await read(RedisConnectionError('down')))
        self.assertIsNone(await read(RedisTimeoutError('slow')))

    async def test_other_errors_propagate(self):
        # Arrange
        @suppress_redis_errors
        async def read() -> int:
            raise ValueError('not redis')

        # Act & Assert
        with self.assertRaises(ValueError):
            await read()

    def test_keeps_the_wrapped_name(self):
        # Arrange
        @suppress_redis_errors
        async def read() -> int:
            return 1

        # Act & Assert
        self.assertEqual(read.__name__, 'read')
