from abc import ABC
from collections.abc import Hashable
from contextlib import AbstractAsyncContextManager
from functools import cached_property
from random import randint
from typing import Any, ClassVar, Generic, Protocol, Self, TypeVar, cast

from ddutils.class_helpers import get_generic_base_argument, require_class_attribute
from ddutils.connections import ConnectionManagerFactory
from ddutils.convertors import convert_camel_case_to_snake_case
from redis.asyncio import Redis

from ddredis.decorators import suppress_redis_errors

JITTER_PERCENT = 10


class Serializable(Protocol):
    """Protocol for objects that can be serialized/deserialized to/from JSON."""

    @classmethod
    def model_validate_json(cls, json_data: str | bytes) -> Self: ...

    def model_dump_json(self) -> str: ...


class Stringable(Protocol):
    def __str__(self) -> str: ...


DomainT = TypeVar('DomainT', bound=Serializable)


class GenericCache(ABC, Generic[DomainT]):
    """
    GenericCache: An async generic caching repository for managing domain objects in Redis.

    This class provides a base implementation for caching domain objects in Redis.
    It supports CRUD operations (`get`, `create`, `update`, `delete`)
    and uses JSON serialization/deserialization to store and retrieve data.

    Domain class must implement `model_validate_json` and `model_dump_json` methods
    (compatible with Pydantic, msgspec, or custom implementations).

    The Redis client is obtained for every call from `client_factory`, a `ConnectionManagerFactory` called
    with `alias`, like a ddsql adapter runs a query inside `factory(alias=...)`. The factory owns the
    lifecycle: which Redis the alias means, the scope the client lives in (one per process and event loop,
    since `redis.asyncio` connections bind to the loop they were opened in), its lazy creation and its
    close on shutdown (`close()`).

    Class Attributes:
        ttl (int): Time-to-live for cache entries in seconds. Default is 5 minutes.
        client_factory: Opens a context handing out the client of `alias`.
        alias: The Redis to use; None picks the factory's default.

    Example Usage:
        client_registries = {
            RedisConnectionAlias.CACHE: ScopedRegistry[Redis](
                create_func=partial(Redis.from_url, 'redis://localhost/0'),
                scope_func=os.getpid,
                destructor_method_name='aclose',
            ),
        }
        redis_client_factory = ConnectionManagerFactory(client_registries, default=RedisConnectionAlias.CACHE)

        class MyDomain(BaseModel):
            id: int
            name: str

        class MyDomainCache(GenericCache[MyDomain]):
            ttl = 10 * 60  # 10 minutes
            client_factory = redis_client_factory

        cache = MyDomainCache()

        domain = MyDomain(id=1, name="Test")
        await cache.create(key=domain.id, value=domain)
        retrieved = await cache.get(key=1)
    """

    _domain_class: ClassVar[type[Serializable]]

    ttl: ClassVar[int] = 5 * 60  # 5 minutes
    client_factory: ClassVar[ConnectionManagerFactory[Any, Redis, Any]]
    alias: ClassVar[Hashable | None] = None

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)

        domain_class = get_generic_base_argument(cls, GenericCache)
        if domain_class is None or isinstance(domain_class, TypeVar):
            raise TypeError(f'{cls.__name__} must specify domain type: class {cls.__name__}(GenericCache[YourDomain])')

        cls._domain_class = domain_class
        require_class_attribute(cls, 'client_factory', ConnectionManagerFactory)

    def _open_client(self) -> AbstractAsyncContextManager[Redis]:
        return self.client_factory(alias=self.alias)

    @cached_property
    def _key_prefix(self) -> str:
        return convert_camel_case_to_snake_case(self._domain_class.__name__)

    @cached_property
    def _jitter(self) -> int:
        return self.ttl * JITTER_PERCENT // 100

    def _generate_key(self, key: Stringable) -> str:
        """Generates a Redis key for the given identifier with the domain-specific prefix."""
        return f':{self._key_prefix}:{key!s}'

    def _generate_ttl(self) -> int:
        """Generates a TTL value with jitter."""
        return self.ttl + randint(-self._jitter, self._jitter)

    async def _get(self, client: Redis, key: str) -> DomainT | None:
        """Reads and deserializes a domain object by its already-prefixed Redis key."""
        cached_data = await client.get(key)
        if not cached_data:
            return None
        try:
            return cast('DomainT', self._domain_class.model_validate_json(cached_data))
        except Exception:  # noqa: BLE001
            return None

    @suppress_redis_errors
    async def get(self, key: Stringable) -> DomainT | None:
        """Retrieves and deserializes a domain object from the cache by its key."""
        async with self._open_client() as client:
            return await self._get(client, self._generate_key(key))

    @suppress_redis_errors
    async def create(self, key: Stringable, value: DomainT) -> None:
        """Caches a new domain object or overwrites an existing one. Sets a TTL for the entry."""
        async with self._open_client() as client:
            await client.set(self._generate_key(key), value.model_dump_json(), ex=self._generate_ttl())

    @suppress_redis_errors
    async def update(self, key: Stringable, value: DomainT) -> None:
        """Updates a domain object in the cache. Alias for `create`."""
        await self.create(key=key, value=value)

    @suppress_redis_errors
    async def delete(self, key: Stringable) -> None:
        """Removes a domain object from the cache by its key."""
        async with self._open_client() as client:
            await client.delete(self._generate_key(key))

    @suppress_redis_errors
    async def get_list(self, key_prefix: Stringable) -> list[DomainT]:
        """Returns all cached domain objects whose keys start with the given prefix."""
        pattern = f'{self._generate_key(key_prefix)}*'
        result: list[DomainT] = []
        async with self._open_client() as client:
            async for redis_key in client.scan_iter(match=pattern):
                item = await self._get(client, redis_key)
                if item is not None:
                    result.append(item)
        return result

    @suppress_redis_errors
    async def delete_by_filters(self, key_prefix: Stringable) -> None:
        """Removes all cached domain objects whose keys start with the given prefix."""
        pattern = f'{self._generate_key(key_prefix)}*'
        async with self._open_client() as client:
            async for redis_key in client.scan_iter(match=pattern):
                await client.delete(redis_key)


__all__ = ('GenericCache', 'Serializable', 'Stringable')
