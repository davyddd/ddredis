import json
import os
from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch

from ddutils.connections import ConnectionManagerFactory
from ddutils.feature_flags import FeatureFlagRecord, FeatureFlags, FeatureFlagStorageError
from ddutils.scoped_registry import ScopedRegistry
from fakeredis.aioredis import FakeRedis
from redis.asyncio import Redis
from redis.exceptions import ConnectionError as RedisConnectionError

from ddredis.feature_flags import RedisFeatureFlagStorage

ON = FeatureFlagRecord(enabled=True, proportion=1.0, payload={'ttl': 10})


class TestRedisFeatureFlagStorage(IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.redis_client = FakeRedis(decode_responses=True)
        self.raw_client = FakeRedis()  # bytes in, bytes out, like a client without decode_responses
        registries = {
            'cache': ScopedRegistry[Redis](create_func=lambda: self.redis_client, scope_func=os.getpid),
            'raw': ScopedRegistry[Redis](create_func=lambda: self.raw_client, scope_func=os.getpid),
        }
        self.factory = ConnectionManagerFactory(registries, default='cache')
        self.storage = RedisFeatureFlagStorage(self.factory)

    async def test_put_get_delete(self):
        # Act
        await self.storage.put('new_checkout', ON)
        record = await self.storage.get('new_checkout')
        deleted = await self.storage.delete('new_checkout')
        deleted_again = await self.storage.delete('new_checkout')

        # Assert
        self.assertEqual(record, ON)
        self.assertTrue(deleted)
        self.assertFalse(deleted_again)
        self.assertIsNone(await self.storage.get('new_checkout'))

    async def test_stored_json_shape(self):
        # Act
        await self.storage.put('on', ON)

        # Assert
        raw = await self.redis_client.hget('feature_flags', 'on')
        assert raw is not None
        self.assertEqual(json.loads(raw), {'enabled': True, 'proportion': 1.0, 'payload': {'ttl': 10}})

    async def test_put_overwrites(self):
        # Arrange
        await self.storage.put('flag', ON)

        # Act
        await self.storage.put('flag', FeatureFlagRecord())

        # Assert
        self.assertEqual(await self.storage.get('flag'), FeatureFlagRecord())

    async def test_get_map_and_malformed_values(self):
        # Arrange
        await self.storage.put('on', ON)
        await self.redis_client.hset('feature_flags', 'broken', 'not json')
        await self.redis_client.hset('feature_flags', 'out_of_range', '{"enabled": true, "proportion": 7}')

        # Act
        with self.assertLogs('ddredis.feature_flags', level='ERROR') as logs:
            records = await self.storage.get_map()
            broken = await self.storage.get('broken')

        # Assert: a malformed value is logged with its name and read as the defaults
        self.assertEqual(records, {'on': ON, 'broken': FeatureFlagRecord(), 'out_of_range': FeatureFlagRecord()})
        self.assertEqual(broken, FeatureFlagRecord())
        self.assertEqual(sorted(entry.msg['flag_name'] for entry in logs.records), ['broken', 'broken', 'out_of_range'])  # type: ignore[index]

    async def test_rename_is_one_transaction(self):
        # Arrange
        await self.storage.put('old', ON)

        # Act
        await self.storage.rename('old', 'new', FeatureFlagRecord(enabled=True, proportion=0.5))

        # Assert
        self.assertEqual(await self.storage.get_map(), {'new': FeatureFlagRecord(enabled=True, proportion=0.5)})

    async def test_alias_and_key_pick_the_hash(self):
        # Arrange
        storage = RedisFeatureFlagStorage(self.factory, alias='raw', key='flags:v2')

        # Act
        await storage.put('on', ON)
        records = await storage.get_map()

        # Assert: bytes names are decoded, the default client was not touched
        self.assertEqual(records, {'on': ON})
        self.assertEqual(await self.raw_client.hkeys('flags:v2'), [b'on'])
        self.assertEqual(await self.redis_client.hkeys('feature_flags'), [])

    async def test_redis_errors_become_storage_errors(self):
        # Act & Assert
        with (
            patch.object(self.redis_client, 'hget', side_effect=RedisConnectionError('down')),
            self.assertRaises(FeatureFlagStorageError) as ctx,
        ):
            await self.storage.get('on')
        self.assertIsInstance(ctx.exception.__cause__, RedisConnectionError)

    async def test_reads_through_feature_flags(self):
        # Arrange
        await self.storage.put('on', ON)
        flags = FeatureFlags(self.storage)

        # Act & Assert
        self.assertTrue(await flags.is_enabled('on'))
        self.assertEqual(await flags.get_payload('on'), {'ttl': 10})
