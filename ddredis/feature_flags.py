import json
import logging
from collections.abc import Hashable
from contextlib import AbstractAsyncContextManager
from typing import Any

from ddutils.connections import ConnectionManagerFactory
from ddutils.feature_flags import FeatureFlagRecord, FeatureFlagStorage, FeatureFlagStorageError
from redis.asyncio import Redis
from redis.exceptions import RedisError

logger = logging.getLogger(__name__)

DEFAULT_FEATURE_FLAGS_KEY = 'feature_flags'


class RedisFeatureFlagStorage(FeatureFlagStorage):
    """
    Feature flags in one Redis hash: the field is the flag name, the value its JSON `{"enabled", "proportion", "payload"}`.

    The client is obtained for every call from `client_factory`, a `ConnectionManagerFactory` called with
    `alias`, like `GenericCache`. `rename` is atomic (a MULTI/EXEC pipeline). A value that is not valid JSON or does
    not validate as a record (edited by hand) is logged and read as the defaults.

    Example:
        feature_flags = FeatureFlags(RedisFeatureFlagStorage(redis_client_factory))
    """

    def __init__(
        self,
        client_factory: ConnectionManagerFactory[Any, Redis, AbstractAsyncContextManager[Redis]],
        *,
        alias: Hashable | None = None,
        key: str = DEFAULT_FEATURE_FLAGS_KEY,
    ) -> None:
        self.client_factory = client_factory
        self.alias = alias
        self.key = key

    def _open_client(self) -> AbstractAsyncContextManager[Redis]:
        return self.client_factory(alias=self.alias)

    @staticmethod
    def _parse(name: str, raw_json: str | bytes) -> FeatureFlagRecord:
        try:
            # ValueError covers JSONDecodeError, UnicodeDecodeError (corrupted bytes in Redis) and a bad record
            return FeatureFlagRecord.from_dict(json.loads(raw_json))
        except ValueError as e:
            logger.error(
                {'message': 'FEATURE_FLAGS: Malformed record, reading the defaults', 'flag_name': name, 'error': str(e)}
            )
            return FeatureFlagRecord()

    @staticmethod
    def _dump(record: FeatureFlagRecord) -> str:
        return json.dumps(record.to_dict())

    @staticmethod
    def _name(field: str | bytes) -> str:
        return field.decode() if isinstance(field, bytes) else field

    async def get(self, name: str) -> FeatureFlagRecord | None:
        try:
            async with self._open_client() as client:
                raw_json = await client.hget(self.key, name)
        except RedisError as e:
            raise FeatureFlagStorageError() from e
        return self._parse(name, raw_json) if raw_json is not None else None

    async def get_map(self) -> dict[str, FeatureFlagRecord]:
        try:
            async with self._open_client() as client:
                entries = await client.hgetall(self.key)
        except RedisError as e:
            raise FeatureFlagStorageError() from e
        return {self._name(name): self._parse(self._name(name), raw_json) for name, raw_json in entries.items()}

    async def put(self, name: str, record: FeatureFlagRecord) -> None:
        try:
            async with self._open_client() as client:
                await client.hset(self.key, name, self._dump(record))
        except RedisError as e:
            raise FeatureFlagStorageError() from e

    async def delete(self, name: str) -> bool:
        try:
            async with self._open_client() as client:
                return bool(await client.hdel(self.key, name))
        except RedisError as e:
            raise FeatureFlagStorageError() from e

    async def rename(self, old_name: str, new_name: str, record: FeatureFlagRecord) -> None:
        try:
            async with self._open_client() as client, client.pipeline(transaction=True) as pipe:
                pipe.hset(self.key, new_name, self._dump(record))
                pipe.hdel(self.key, old_name)
                await pipe.execute()
        except RedisError as e:
            raise FeatureFlagStorageError() from e


__all__ = ('DEFAULT_FEATURE_FLAGS_KEY', 'RedisFeatureFlagStorage')
