# ddredis

[![pypi](https://img.shields.io/pypi/v/ddredis.svg)](https://pypi.python.org/pypi/ddredis)
[![downloads](https://static.pepy.tech/badge/ddredis/month)](https://pepy.tech/project/ddredis)
[![versions](https://img.shields.io/pypi/pyversions/ddredis.svg)](https://github.com/davyddd/ddredis)
[![codecov](https://codecov.io/gh/davyddd/ddredis/branch/main/graph/badge.svg)](https://app.codecov.io/github/davyddd/ddredis)
[![license](https://img.shields.io/github/license/davyddd/ddredis.svg)](https://github.com/davyddd/ddredis/blob/main/LICENSE)

Redis helpers: generic JSON cache and distributed lock

## Installation

Install the library using pip:
```bash
pip install ddredis
```

## Development

The project runs entirely in Docker. Requires Docker and [Fabric](https://www.fabfile.org/) on the host:

```bash
fab build      # build the dev image
fab tests      # run pytest
fab linters    # run ruff (with --fix), ty and complexipy
fab shell      # IPython inside the container
fab bash       # bash inside the container
```

This project was generated from [dd-lib-stub](https://github.com/davyddd/dd-lib-stub);
run `copier update` to pull in template updates.

## GenericCache

Caches domain objects in Redis as JSON. The domain class must provide `model_validate_json` and
`model_dump_json` (pydantic models do), keys are prefixed with the snake-cased domain name, every entry gets
the class `ttl` with ±10% jitter so a burst of writes does not expire at once, and Redis errors turn into
"not found" instead of failing the caller.

The client is obtained for every call from `client_factory`, a `ConnectionManagerFactory` from
[ddutils](https://github.com/davyddd/ddutils) called with `alias`, the way a [ddsql](https://github.com/davyddd/ddsql)
adapter runs a query inside `factory(alias=...)`: the factory owns which Redis the alias means, the scope the
client lives in (one `ScopedRegistry` per alias, a client per process and event loop, since `redis.asyncio`
connections bind to the loop they were opened in), its lazy creation and its close on shutdown.

```python
import asyncio
import os
from enum import Enum
from functools import partial
from typing import ClassVar

from ddutils.connections import ConnectionManagerFactory
from ddutils.scoped_registry import ScopedRegistry
from pydantic import BaseModel
from redis.asyncio import Redis

from ddredis.cache import GenericCache


class RedisConnectionAlias(str, Enum):
    CACHE = 'cache'


def client_scope() -> tuple[int, asyncio.AbstractEventLoop]:
    return os.getpid(), asyncio.get_running_loop()


client_registries = {
    RedisConnectionAlias.CACHE: ScopedRegistry[Redis](
        create_func=partial(Redis.from_url, 'redis://localhost/0'),
        scope_func=client_scope,
        destructor_method_name='aclose',
    ),
}
redis_client_factory = ConnectionManagerFactory(client_registries, default=RedisConnectionAlias.CACHE)


class Profile(BaseModel):
    profile_id: int
    name: str


class ProfileCache(GenericCache[Profile]):
    ttl = 10 * 60  # seconds
    client_factory = redis_client_factory
    # alias = RedisConnectionAlias.CACHE  # None picks the factory's default


cache = ProfileCache()

await cache.create(key=1, value=Profile(profile_id=1, name='John'))   # SET :profile:1 ... EX ttl±10%
profile = await cache.get(key=1)                                       # Profile | None
await cache.update(key=1, value=profile)                               # alias for create
await cache.delete(key=1)

await cache.get_list(key_prefix='team-a')                              # every :profile:team-a* entry
await cache.delete_by_filters(key_prefix='team-a')

await redis_client_factory.close()                                     # on shutdown
```

Redis errors are swallowed on purpose: for a cache, "unavailable" and "miss" lead to the same fallback. Do
not use the cache for state where `None` has a meaning of its own. Transient failures (a worker that was just
reloaded, a dropped connection) belong to the client, not to the cache: redis-py retries the errors listed in
`retry_on_error` with its default `Retry` (3 attempts, exponential backoff with jitter), so a production
`create_func` looks like this:

```python
from redis.asyncio import Redis
from redis.exceptions import ConnectionError, TimeoutError

create_client = partial(
    Redis.from_url,
    'redis://localhost/0',
    max_connections=20,
    socket_connect_timeout=5,
    socket_timeout=1,
    retry_on_error=[ConnectionError, TimeoutError],
    decode_responses=True,
)
```

## RedisLock

Distributed lock on `SET NX PX` with a Lua release (redis-py's `Lock`, `thread_local=False`). Acquiring a
held lock raises `AlreadyAcquiredError` unless `blocking=True`, in which case it waits up to `timeout`.

```python
from ddredis.lock import AlreadyAcquiredError, RedisLock

try:
    async with redis_client_factory() as redis_client, RedisLock(redis_client, 'sync-profiles', timeout=30):
        await sync_profiles()
except AlreadyAcquiredError:
    pass  # another worker is on it
```

`timeout` is the Redis-side TTL of the key, not a client-side cancellation: if the block runs longer, Redis
drops the key and a competitor may enter while the first holder is still running. Pick a timeout that
comfortably exceeds the worst-case runtime, or call `extend()` periodically as a heartbeat. `extend()`
replaces the TTL with the full `timeout` counted from now rather than adding to the remaining time, so a
long-lived holder beating steadily never pushes its expiry further out. `release()` tolerates a lock that
already expired.

## suppress_redis_errors

Decorator for async functions: `RedisError` subclasses become a `None` result, everything else propagates.
`GenericCache` methods use it; apply it to your own Redis reads that should degrade instead of failing.

