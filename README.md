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
run `uvx copier update` to pull in template updates.

## Compatibility

Python 3.11 to 3.15. What changed in each release, breaking changes included,
is in the [GitHub releases](https://github.com/davyddd/ddredis/releases).

<!-- The sections below are the library's own; copier update leaves them alone -->

Depends on `redis>=6,<9` and `ddutils>=0.4.0,<0.5.0`. The tests run on fakeredis, no server is needed.

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
from enum import Enum
from functools import partial

from ddutils.connections import ConnectionManagerFactory
from ddutils.scoped_registry import ScopedRegistry, process_loop_scope
from pydantic import BaseModel
from redis.asyncio import Redis

from ddredis.cache import GenericCache


class RedisConnectionAlias(str, Enum):
    CACHE = 'cache'


client_registries = {
    RedisConnectionAlias.CACHE: ScopedRegistry[Redis](
        create_func=partial(Redis.from_url, 'redis://localhost/0', decode_responses=True),
        scope_func=process_loop_scope,
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

A subclass binds the domain class (`GenericCache[Profile]`; `TypeError` for a bare or still generic base) and
declares `client_factory`, a `ConnectionManagerFactory` (`AttributeError` when missing, `TypeError` when something
else), both checked at class definition. `alias` defaults to the factory's default, `ttl` to 300 seconds; the
jitter is `ttl // 10`, so a `ttl` under 10 seconds has none.

Keys are `:<snake_case_domain>:<key>`, with `str(key)` of whatever is passed; `get_list(key_prefix)` and
`delete_by_filters(key_prefix)` run `SCAN MATCH :<prefix>:<key_prefix>*`, so `key_prefix` is a glob and the cost
is the whole keyspace, not the matches. An entry that does not deserialize into the domain class reads as a miss
and is skipped by `get_list`. The client may run with or without `decode_responses`.

Redis errors are swallowed on purpose: for a cache, "unavailable" and "miss" lead to the same fallback. Do
not use the cache for state where `None` has a meaning of its own. Transient failures (a worker that was just
reloaded, a dropped connection) belong to the client, not to the cache: redis-py 6+ retries the errors listed in
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

`RedisLock(client_factory, key, timeout=10.0, blocking=False, *, alias=None)`: a distributed lock on `SET NX PX`
with a Lua release (redis-py's `Lock`, `thread_local=False`). The client comes from a `ConnectionManagerFactory`
like the cache's, borrowed for every call: the object holds only the token that identifies this holder, and a new
redis-py `Lock` is built around the client each time. `key` is the Redis key as given, no prefix. Acquiring a held
lock raises `AlreadyAcquiredError`; with `blocking=True` it waits up to `timeout` and raises the same error when
the wait runs out. The lock is not reentrant, but the object can be acquired again after `release()`.

```python
from ddredis.lock import AlreadyAcquiredError, RedisLock

try:
    async with RedisLock(redis_client_factory, 'sync-profiles', timeout=30):
        await sync_profiles()
except AlreadyAcquiredError:
    pass  # another worker is on it
```

`timeout` is the Redis-side TTL of the key, not a client-side cancellation: if the block runs longer, Redis
drops the key and a competitor may enter while the first holder is still running. Pick a timeout that
comfortably exceeds the worst-case runtime, or call `extend()` periodically as a heartbeat. `extend()`
replaces the TTL with the full `timeout` counted from now rather than adding to the remaining time, so a
long-lived holder beating steadily never pushes its expiry further out. `release()` tolerates a lock that
already expired, `extend()` on one raises redis-py's `LockNotOwnedError`; either without a prior `acquire()`
raises redis-py's `LockError` (both from `redis.asyncio.lock`, not re-exported).

## RedisFeatureFlagStorage

The Redis backend of [ddutils](https://github.com/davyddd/ddutils) feature flags:
`RedisFeatureFlagStorage(client_factory, *, alias=None, key='feature_flags')`, one hash under `key`
(`DEFAULT_FEATURE_FLAGS_KEY`), the field is the flag name, the value its JSON record
`{"enabled": true, "proportion": 1.0, "payload": {}}`. The client comes from a `ConnectionManagerFactory` like
the cache's, with or without `decode_responses`; `rename` is one MULTI/EXEC pipeline. Redis errors raise
`FeatureFlagStorageError`, so `FeatureFlags` keeps serving the last known values while Redis is down.

A value that is not JSON or not a valid record is logged with an ERROR entry and read as the defaults, which
means off: a record broken by hand disables its flag. The parsing is the strict `FeatureFlagRecord.from_dict`,
an extra key is as malformed as a wrong value, while `enabled` accepts what the record accepts (`true`, `1`,
`"yes"`, ...). `get_map` keeps the valid records when one is malformed.

```python
from ddredis.feature_flags import RedisFeatureFlagStorage
from ddutils.feature_flags import FeatureFlags

feature_flags = FeatureFlags(RedisFeatureFlagStorage(redis_client_factory))  # alias=..., key='feature_flags'

if await feature_flags.is_enabled('new_checkout'):            # default proportion: on or off for everyone
    ...
if await feature_flags.is_enabled('new_ranking', user_id):  # on for `proportion` of the users
    ...
```
