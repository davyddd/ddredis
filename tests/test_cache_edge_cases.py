import json
from dataclasses import asdict, dataclass
from typing import ClassVar, Self
from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch
from uuid import UUID

from fakeredis.aioredis import FakeRedis
from pydantic import BaseModel
from redis.asyncio import Redis
from redis.exceptions import ConnectionError as RedisConnectionError

from ddredis.cache import GenericCache

redis_client = FakeRedis()


class UserProfile(BaseModel):
    user_id: int


class UserProfileCache(GenericCache[UserProfile]):
    ttl = 5  # jitter of 10% rounds down to 0
    redis_client: ClassVar[Redis] = redis_client


class Order(BaseModel):
    order_id: int


class OrderCache(GenericCache[Order]):
    redis_client: ClassVar[Redis] = redis_client


@dataclass
class PlainEvent:
    """Serializable without pydantic: the protocol is the only contract."""

    name: str

    @classmethod
    def model_validate_json(cls, json_data: str | bytes) -> Self:
        return cls(**json.loads(json_data))

    def model_dump_json(self) -> str:
        return json.dumps(asdict(self))


class PlainEventCache(GenericCache[PlainEvent]):
    redis_client: ClassVar[Redis] = redis_client


class TestGenericCacheEdgeCases(IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        await redis_client.flushall()

    def test_key_prefix_is_snake_cased_domain_name(self):
        # Act & Assert
        self.assertEqual(UserProfileCache()._generate_key(1), ':user_profile:1')
        self.assertEqual(OrderCache()._generate_key(1), ':order:1')

    def test_any_stringable_key(self):
        # Arrange
        cache = OrderCache()
        uuid_key = UUID('12345678-1234-5678-1234-567812345678')

        # Act & Assert
        self.assertEqual(cache._generate_key(uuid_key), ':order:12345678-1234-5678-1234-567812345678')
        self.assertEqual(cache._generate_key('composite:key'), ':order:composite:key')

    def test_small_ttl_has_no_jitter(self):
        # Act & Assert
        self.assertEqual({UserProfileCache()._generate_ttl() for _ in range(20)}, {5})

    async def test_caches_on_one_redis_are_isolated_by_prefix(self):
        # Arrange
        await UserProfileCache().create(key=1, value=UserProfile(user_id=1))
        await OrderCache().create(key=1, value=Order(order_id=1))

        # Act
        await OrderCache().delete_by_filters(key_prefix='')

        # Assert
        self.assertIsNone(await OrderCache().get(key=1))
        self.assertEqual(await UserProfileCache().get(key=1), UserProfile(user_id=1))

    async def test_domain_class_without_pydantic(self):
        # Arrange
        cache = PlainEventCache()

        # Act
        await cache.create(key='signup', value=PlainEvent(name='signup'))
        cached = await cache.get(key='signup')

        # Assert
        self.assertEqual(cached, PlainEvent(name='signup'))
        self.assertEqual(await redis_client.get(':plain_event:signup'), b'{"name": "signup"}')

    async def test_get_list_skips_unparseable_entries(self):
        # Arrange
        cache = OrderCache()
        await cache.create(key='a:1', value=Order(order_id=1))
        await redis_client.set(':order:a:2', 'garbage')

        # Act
        orders = await cache.get_list(key_prefix='a')

        # Assert
        assert orders is not None
        self.assertEqual(orders, [Order(order_id=1)])

    async def test_get_list_without_matches(self):
        # Act
        orders = await OrderCache().get_list(key_prefix='nothing')

        # Assert
        self.assertEqual(orders, [])

    async def test_write_and_delete_errors_are_suppressed(self):
        # Arrange
        cache = OrderCache()
        order = Order(order_id=1)

        # Act & Assert: every public method degrades to None instead of raising
        with patch.object(redis_client, 'set', side_effect=RedisConnectionError('down')):
            self.assertIsNone(await cache.create(key=1, value=order))
            self.assertIsNone(await cache.update(key=1, value=order))
        with patch.object(redis_client, 'delete', side_effect=RedisConnectionError('down')):
            self.assertIsNone(await cache.delete(key=1))
        with patch.object(redis_client, 'scan_iter', side_effect=RedisConnectionError('down')):
            self.assertIsNone(await cache.get_list(key_prefix='a'))
            self.assertIsNone(await cache.delete_by_filters(key_prefix='a'))

    def test_generic_subclass_must_bind_the_domain_type(self):
        # Act & Assert: a still-generic intermediate class is not a usable cache
        with self.assertRaises(TypeError):

            class StillGeneric[T: BaseModel](GenericCache[T]):
                redis_client: ClassVar[Redis] = redis_client
