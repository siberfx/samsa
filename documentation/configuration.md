# Configuration

Publish the config file with:

```bash
php artisan vendor:publish --tag=samsa-config
```

This page documents every key in `config/samsa.php`.

- [Default driver](#default-driver)
- [Drivers](#drivers)
- [Topic defaults and topics](#topic-defaults-and-topics)
- [Consumer](#consumer)
- [Retry](#retry)
- [Dead letters](#dead-letters)
- [Schedule](#schedule)
- [Event bridge](#event-bridge)
- [message-bus transport](#message-bus-transport)
- [Serializer and partitioner](#serializer-and-partitioner)
- [Subscriptions](#subscriptions)
- [Queue connection](#queue-connection)
- [Environment variables](#environment-variables)

## Default driver

```php
'default' => env('SAMSA_DRIVER', 'database'),
```

`database`, `nats`, `redis` or `memory`. You can address another driver at runtime with `Samsa::driver('nats')->produce(...)`, and every Artisan command accepts `--driver=`.

## Drivers

```php
'drivers' => [
    'database' => [
        'connection'   => env('SAMSA_DB_CONNECTION'), // null = app default connection
        'table_prefix' => '',                         // prepended to samsa_* tables
    ],

    'nats' => [
        'host'      => env('NATS_HOST', '127.0.0.1'),
        'port'      => (int) env('NATS_PORT', 4222),
        'user'      => env('NATS_USER'),
        'pass'      => env('NATS_PASS'),
        'token'     => env('NATS_TOKEN'),
        'timeout'   => 1.0,          // connect/request timeout in seconds
        'reconnect' => true,
        'storage'   => 'file',       // file | memory — JetStream storage for new streams
        'replicas'  => 1,            // 1, 3 or 5 (needs a cluster for > 1)
        'prefix'    => env('SAMSA_NATS_PREFIX', 'samsa'), // subject root and stream prefix
        'duplicate_window' => 120, // seconds: dedup keys are honoured within this window
    ],

    'redis' => [
        'connection' => env('SAMSA_REDIS_CONNECTION', 'default'), // from config/database.php
        'prefix'     => 'samsa:',
    ],

    'memory' => [],
],
```

Per-driver details: [Database](drivers/database.md) · [NATS](drivers/nats.md) · [Redis](drivers/redis-streams.md).

## Topic defaults and topics

```php
'defaults' => [
    'partitions' => 8,
    'retention' => [
        'max_age_seconds'           => 60 * 60 * 24 * 7, // null = keep forever
        'max_records_per_partition' => null,
        'respect_slowest_consumer'  => false,
    ],
],

'topics' => [
    'orders'     => ['partitions' => 16, 'retention' => ['max_age_seconds' => 60 * 60 * 24 * 30]],
    'page-views' => ['partitions' => 4,  'retention' => ['max_age_seconds' => 60 * 60 * 24]],
],

'auto_create_topics' => env('SAMSA_AUTO_CREATE_TOPICS', false),
```

| Key | Meaning |
|---|---|
| `defaults.partitions` | Partition count for topics created without an explicit one. |
| `defaults.retention.*` | Retention for topics without their own. See [Concepts → Retention](concepts.md#retention). |
| `topics` | Declarative topology. `samsa:topic:create --from-config` creates or updates them (partition counts never shrink; growing needs `--force`). |
| `auto_create_topics` | Create an unknown topic on first produce using `defaults`. Convenient locally, but **keep it off in production**, where a typo would silently create a new topic. |

Topic names must match `[a-z0-9._-]+`.

## Consumer

```php
'consumer' => [
    'batch_size'                => 100,
    'poll_interval_ms'          => 250,
    'lease_seconds'             => 30,
    'heartbeat_seconds'         => 10,
    'commit'                    => 'batch',   // batch | record
    'start_from'                => 'earliest', // earliest | latest — for a group's first run
    'max_partitions_per_worker' => null,      // null = fair share
],
```

| Key | Meaning | Tuning advice |
|---|---|---|
| `batch_size` | Records fetched per partition per read. | Raise for throughput; lower if handlers are slow and you want faster rebalances. |
| `poll_interval_ms` | Sleep when a worker has no new records. | Lower means lower latency but more queries when idle. |
| `lease_seconds` | How long a partition stays claimed without renewal. | It's also the **takeover delay after a crash**. Must be longer than your slowest single record. |
| `heartbeat_seconds` | How often leases and membership are renewed. | Keep below `lease_seconds / 2`. |
| `commit` | `batch` commits after each batch, `record` after each record. | `record` means fewer redeliveries after a crash, but more writes. |
| `start_from` | Where a brand-new group starts. | `latest` for groups that only care about new events. |
| `max_partitions_per_worker` | Cap per worker. | Useful when a few workers run on a small server. |

All of these can be overridden per run with `samsa:consume` options. See [Consuming](consuming.md#worker-options).

## Retry

```php
'retry' => [
    'max_attempts' => 3,
    'backoff_ms'   => [1000, 5000, 15000], // wait before attempt 2, 3, 4…; last value repeats
],
```

Override per handler with the `#[Retry]` attribute. See [Consuming → Retries](consuming.md#retries).

> The worker holds a partition while it waits between retries, so `sum(backoff_ms)` plus handler time should stay well below `lease_seconds`. Samsa renews the lease during backoff waits, but a very long backoff still blocks the partition for its group.

## Dead letters

```php
'dead_letter' => [
    'enabled' => true,
],
```

If disabled, a record that exhausts its retries **stops the worker** with an exception instead of being skipped. Choose this only when skipping a record is never acceptable.

## Schedule

```php
'schedule' => [
    'prune' => env('SAMSA_SCHEDULE_PRUNE', true), // register samsa:prune hourly
    'release_delayed' => env('SAMSA_SCHEDULE_DELAYED'),
],
```

`release_delayed` registers `samsa:release-delayed` every second. `null` (the default) enables it automatically when a queue connection uses the `samsa` driver; `true` or `false` force it. Delayed records (from `Queue::later()`, `$job->release($delay)` and message-bus `DelayStamp`s) wait in the driver's delayed store (the `samsa_delayed` table for the Database driver) until it runs. See [Queue driver](queue-driver.md#delayed-jobs).


## Event bridge

```php
'bridge' => [
    'enabled'                  => true,
    'forward_broadcast_events' => false,
    'broadcast_topic'          => 'broadcasts',
],
```

See [Event bridge](event-bridge.md).

## message-bus transport

Nothing to configure here. When `siberfx/message-bus` is installed, the `samsa` transport driver is registered automatically and configured in `config/message-bus.php`. See [message-bus transport](message-bus-integration.md).

## Serializer and partitioner

```php
'serializer'  => \Siberfx\Samsa\Serialization\JsonSerializer::class,
'partitioner' => \Siberfx\Samsa\Producer\DefaultPartitioner::class,
```

- **Serializer:** payloads and headers are stored as JSON. Samsa never uses PHP `unserialize()` on stored data. Implement `Siberfx\Samsa\Contracts\Serializer` to change this (for example to add encryption).
- **Partitioner:** implement `Siberfx\Samsa\Contracts\Partitioner` to route keys yourself, for example pinning a tenant to a partition. Changing the partitioner of an existing topic has the same effect as changing its partition count: key ordering breaks for in-flight records.

## Subscriptions

```php
'subscriptions' => [
    'billing' => [
        'orders'  => \App\Samsa\Handlers\ChargeOrder::class,
        'refunds' => \App\Samsa\Handlers\IssueRefund::class,
    ],
    'emails' => [
        'orders' => \App\Samsa\Handlers\SendOrderConfirmation::class,
    ],
],
```

Group → topic → handler class. Equivalent to calling `Samsa::subscribe()` in a service provider. See [Consuming → Handlers](consuming.md#handlers).

## Queue connection

To run Laravel jobs on Samsa, add a connection to `config/queue.php`:

```php
'samsa' => [
    'driver'       => 'samsa',
    'topic'        => 'laravel-jobs',
    'group'        => 'laravel-queue',
    'after_commit' => false,
    'lease_seconds' => 90,          // a job must finish within its lease
    'samsa_driver'  => null,        // null = samsa.default
],
```

See [Queue driver](queue-driver.md).

## Environment variables

| Variable | Default | Used by |
|---|---|---|
| `SAMSA_DRIVER` | `database` | `default` |
| `SAMSA_DB_CONNECTION` | app default | Database driver |
| `SAMSA_REDIS_CONNECTION` | `default` | Redis driver |
| `SAMSA_AUTO_CREATE_TOPICS` | `false` | `auto_create_topics` |
| `NATS_HOST` / `NATS_PORT` | `127.0.0.1` / `4222` | NATS driver |
| `NATS_USER` / `NATS_PASS` / `NATS_TOKEN` | — | NATS driver auth |
