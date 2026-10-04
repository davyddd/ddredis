from typing import ClassVar
from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch

from fakeredis.aioredis import FakeRedis
from pydantic import BaseModel
from redis.asyncio import Redis
from redis.exceptions import ConnectionError as RedisConnectionError

from ddredis.cache import GenericCache


class Profile(BaseModel):
    profile_id: int
    name: str


TTL = 100
JITTER = TTL * 10 // 100


class ProfileCache(GenericCache[Profile]):
    ttl = TTL
    redis_client: ClassVar[Redis] = FakeRedis()


class TestGenericCache(IsolatedAsyncioTestCase):
    def setUp(self):
        self.cache = ProfileCache()
        self.profile = Profile(profile_id=1, name='John')

    async def asyncSetUp(self):
        await ProfileCache.redis_client.flushall()

    def test_subclass_must_declare_domain_type(self):
        # Act & Assert
        with self.assertRaises(TypeError):

            class UntypedCache(GenericCache):  # type: ignore[type-arg]
                redis_client: ClassVar[Redis] = FakeRedis()

    def test_key_is_prefixed_with_the_domain_name(self):
        # Act & Assert
        self.assertEqual(self.cache._generate_key(1), ':profile:1')

    def test_ttl_has_jitter_within_ten_percent(self):
        # Act
        ttls = {self.cache._generate_ttl() for _ in range(200)}

        # Assert
        self.assertTrue(all(TTL - JITTER <= ttl <= TTL + JITTER for ttl in ttls), ttls)
        self.assertGreater(len(ttls), 1)

    async def test_create_and_get(self):
        # Act
        await self.cache.create(key=self.profile.profile_id, value=self.profile)
        cached = await self.cache.get(key=1)

        # Assert
        self.assertEqual(cached, self.profile)
        self.assertIsInstance(cached, Profile)
        ttl = await ProfileCache.redis_client.ttl(':profile:1')
        self.assertTrue(TTL - JITTER <= ttl <= TTL + JITTER, ttl)

    async def test_get_missing_key(self):
        # Act & Assert
        self.assertIsNone(await self.cache.get(key=404))

    async def test_get_ignores_unparseable_payload(self):
        # Arrange
        await ProfileCache.redis_client.set(':profile:1', 'not json')

        # Act & Assert
        self.assertIsNone(await self.cache.get(key=1))

    async def test_update_overwrites(self):
        # Arrange
        await self.cache.create(key=1, value=self.profile)

        # Act
        await self.cache.update(key=1, value=Profile(profile_id=1, name='Jane'))

        # Assert
        cached = await self.cache.get(key=1)
        assert cached is not None
        self.assertEqual(cached.name, 'Jane')

    async def test_delete(self):
        # Arrange
        await self.cache.create(key=1, value=self.profile)

        # Act
        await self.cache.delete(key=1)

        # Assert
        self.assertIsNone(await self.cache.get(key=1))

    async def test_get_list_and_delete_by_prefix(self):
        # Arrange
        await self.cache.create(key='team-a:1', value=Profile(profile_id=1, name='A1'))
        await self.cache.create(key='team-a:2', value=Profile(profile_id=2, name='A2'))
        await self.cache.create(key='team-b:1', value=Profile(profile_id=3, name='B1'))

        # Act
        team_a = await self.cache.get_list(key_prefix='team-a')
        await self.cache.delete_by_filters(key_prefix='team-a')
        team_a_after = await self.cache.get_list(key_prefix='team-a')
        team_b = await self.cache.get_list(key_prefix='team-b')

        # Assert
        assert team_a is not None and team_a_after is not None and team_b is not None
        self.assertEqual(sorted(profile.name for profile in team_a), ['A1', 'A2'])
        self.assertEqual(team_a_after, [])
        self.assertEqual(len(team_b), 1)

    async def test_redis_errors_are_suppressed(self):
        # Arrange
        with patch.object(ProfileCache.redis_client, 'get', side_effect=RedisConnectionError('down')):
            # Act & Assert
            self.assertIsNone(await self.cache.get(key=1))
