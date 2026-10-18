# Redis Streams Driver

Uses [Redis Streams](https://redis.io/docs/latest/develop/data-types/streams/) for the log, and plain hashes and sorted sets for offsets, leases and members. It's a good fit when Redis is already a **durable** part of your infrastructure.

- [Why use it](#why-use-it)
- [Durability first](#durability-first)
- [Setup](#setup)
- [How Samsa maps to Redis](#how-samsa-maps-to-redis)
- [Offsets](#offsets)
- [Groups, leases and commits](#groups-leases-and-commits)
- [Deduplication](#deduplication)
- [Retention](#retention)
- [Limitations](#limitations)
- [Monitoring](#monitoring)

## Why use it

- You probably already run Redis.
- High throughput and low latency.
- Works with both `phpredis` and `predis`, through Laravel's Redis connections.
- Every Samsa feature works the same: consumer groups, rebalancing, fencing, retries, dead letters, seek, prune, the queue driver, the event bridge and the message-bus transport.

The trade-offs: no transactional produce with your database, and durability depends entirely on how Redis is configured.

## Durability first

Redis is often run as a cache, with eviction and no persistence. **That configuration will lose your log.** For Samsa, use:

```conf
appendonly yes
appendfsync everysec          # or "always" for stronger guarantees
maxmemory-policy noeviction   # never evict stream keys
```

Or use a dedicated Redis instance just for Samsa. Redis 6.2 or newer is required.

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

```php
// config/samsa.php
'redis' => [
    'connection' => env('SAMSA_REDIS_CONNECTION', 'default'),
    'prefix'     => 'samsa:',  // every Samsa key starts with this
    'dedup_ttl'  => 86400,     // seconds a dedup key is remembered
],
```

```bash
php artisan samsa:topic:create orders --partitions=8 --driver=redis
```

Samsa sends raw commands, so the Redis connection's own key `prefix` option doesn't apply to Samsa's keys. Only `samsa.drivers.redis.prefix` does.

## How Samsa maps to Redis

With the default prefix `samsa:`:

| Samsa | Redis |
|---|---|
| Topic metadata | Hash `samsa:topics` (field = topic) |
| Partition 3 of `orders` | Stream `samsa:log:orders:3` |
| Offset | Entry ID, e.g. `1729270000000-0` |
| Read after offset N | `XRANGE samsa:log:orders:3 (N + COUNT n` |
| Payload, headers, key, event type, created-at | Entry fields `p`, `h`, `k`, `t`, `c` |
| Committed offsets | Hash `samsa:offsets` (field = `group\|topic\|partition`) |
| Leases | Hash `samsa:leases:{group}:{topic}` (field = partition, value = `member\|until`) |
| Members | Sorted set `samsa:members:{group}:{topic}` (score = expiry) |
| Dedup key | `samsa:dedup:{topic}:{hash}` with a TTL |
| Dead letters | Hash `samsa:dlq` |
| Delayed records | Sorted set `samsa:delayed` + hash `samsa:delayed:data` |

Names that aren't plain `[A-Za-z0-9_.-]` are hex-encoded behind a `~` inside keys.

## Offsets

Offsets are Redis **entry IDs** (`<ms>-<seq>`). They're strings and are compared by the driver, so don't parse them. Within a partition they always increase.

## Groups, leases and commits

Samsa doesn't use Redis consumer groups (`XREADGROUP`). It reads by ID and keeps the group position itself, exactly like the other drivers. That's what keeps per-key ordering, seek, replay and fencing identical:

- **Leases** are claimed and released with Lua scripts, so each check-and-write is atomic.
- **Commits are fenced atomically:** one Lua script checks the member's lease and writes the offset.
- Lease expiry uses the workers' clocks, so keep worker machines NTP-synced.

## Deduplication

A dedup key is checked and set in the same Lua script as the `XADD`. A duplicate produce within `dedup_ttl` (default 24 hours) returns the original record's metadata.

## Retention

| Samsa retention | Redis |
|---|---|
| `max_records_per_partition` | `XADD … MAXLEN ~ N` on every produce (approximate, cheap) |
| `max_age_seconds` | `samsa:prune` runs `XTRIM … MINID` (schedule it; it's registered hourly by default) |
| `respect_slowest_consumer` | No `MAXLEN` on produce; `samsa:prune` never trims past the slowest group's commit |

## Limitations

- **Redis Cluster is not supported yet.** The lease and fenced-commit scripts touch two keys that may live in different slots. Use a single primary, with replicas or Sentinel if you need failover.
- Lag (`samsa:status`, `size()`) is counted with `XRANGE` scans, which is fine for typical backlogs. With millions of uncommitted records per partition, status checks get slower.
- Seeking by time scans entries' `c` field in chunks.

## Monitoring

- `samsa:status --driver=redis` shows lag and owners like the other drivers.
- `XINFO STREAM samsa:log:orders:3` shows length, first and last entry.
- Watch `used_memory` against `maxmemory`. Streams live in RAM.
