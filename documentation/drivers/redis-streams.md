# Redis Streams Driver

> **Planned for v1.3.** This page documents the design of a feature that is not in 1.0.0 yet. Follow the [changelog](../../CHANGELOG.md) for its release.

Uses [Redis Streams](https://redis.io/docs/latest/develop/data-types/streams/) (`XADD`, `XREADGROUP`, `XACK`) for the log and plain Redis keys for leases. It's a good fit when Redis is already a **durable** part of your infrastructure.

- [Why use it](#why-use-it)
- [Durability first](#durability-first)
- [Setup](#setup)
- [How Samsa maps to Redis](#how-samsa-maps-to-redis)
- [Crash recovery](#crash-recovery)
- [Retention](#retention)
- [Redis Cluster](#redis-cluster)
- [Monitoring](#monitoring)

## Why use it

- You probably already run Redis.
- High throughput and low latency.
- Works with both `phpredis` and `predis`, through Laravel's Redis connections.

The trade-offs: no transactional produce with your database, and durability depends on how Redis is configured.

## Durability first

Redis is often run as a cache, with eviction and no persistence. **That configuration will lose your log.** For Samsa, use:

```conf
appendonly yes
appendfsync everysec          # or "always" for stronger guarantees
maxmemory-policy noeviction   # never evict stream keys
```

Or use a dedicated Redis instance just for Samsa. Samsa checks `maxmemory-policy` on `samsa:topic:create` and warns if it isn't `noeviction`.

## Setup

```dotenv
SAMSA_DRIVER=redis
SAMSA_REDIS_CONNECTION=samsa
```

```php
// config/database.php → 'redis'
'samsa' => [
    'host'     => env('REDIS_HOST', '127.0.0.1'),
    'port'     => env('REDIS_PORT', 6379),
    'password' => env('REDIS_PASSWORD'),
    'database' => 3,
],
```

```bash
php artisan samsa:topic:create orders --partitions=8 --driver=redis
```

## How Samsa maps to Redis

With the default prefix `samsa:`:

| Samsa | Redis |
|---|---|
| Topic metadata | Hash `samsa:topics` |
| Partition 3 of `orders` | Stream `samsa:orders:p3` |
| Offset | Entry ID, e.g. `1729270000000-0` |
| Group `billing` | Consumer group `billing` on each partition stream |
| Fetch | `XREADGROUP GROUP billing <member> COUNT n STREAMS samsa:orders:p3 >` |
| Commit | `XACK` of every delivered ID up to the committed one |
| Seek | `XGROUP SETID` |
| Lease | `SET samsa:lease:billing:orders:3 <member> NX PX <ttl>`, renewed and released with compare-and-set Lua scripts |
| Dedup key | `SET samsa:dedup:orders:<key> 1 NX EX 86400` before `XADD` (atomic in one Lua script) |
| Dead letters | Stream `samsa:orders:dlq` |

Offsets are Redis entry IDs (`<ms>-<seq>`). They're strings and are compared by the driver, so don't parse them.

## Crash recovery

Redis consumer groups track a **pending entries list** (PEL): entries delivered but not acked. When a worker takes over a partition, it first claims pending entries of the previous owner (`XAUTOCLAIM`) and processes them **before** reading new ones. That preserves order and at-least-once delivery.

## Retention

| Samsa retention | Redis |
|---|---|
| `max_records_per_partition` | `XADD … MAXLEN ~ N` on every produce (approximate, cheap) |
| `max_age_seconds` | `samsa:prune` runs `XTRIM … MINID ~ <now - age>` |
| `respect_slowest_consumer` | `samsa:prune` never trims past the lowest committed ID of any group |

## Redis Cluster

Each partition is its own key, so partitions spread across cluster slots naturally. When the connection is a cluster, Samsa writes keys with hash tags (`samsa:{orders:p3}`, `samsa:lease:{orders:p3}:billing`) so that each Lua script only touches keys in one slot.

## Monitoring

- `samsa:status --driver=redis` uses `XINFO GROUPS` and `XPENDING` for lag and pending counts.
- `XINFO STREAM samsa:orders:p3` shows length, first and last entry.
- Watch `used_memory` against `maxmemory`. Streams live in RAM.
