<h1 align="center">Samsa</h1>

<p align="center">
  <strong>Kafka's model of topics, partitions, offsets and consumer groups — inside your Laravel app.</strong><br>
  Pure PHP. No broker required. No C extensions.
</p>

<p align="center">
  <a href="documentation/getting-started.md">Getting started</a> ·
  <a href="documentation/concepts.md">Concepts</a> ·
  <a href="documentation/README.md">Documentation</a> ·
  <a href="documentation/faq.md">FAQ</a>
</p>

---

> **Status: in development.** The Database and memory drivers, producers, consumer groups, workers, retries, dead letters and the operations commands are implemented and tested on SQLite, MySQL/MariaDB and PostgreSQL (features marked ✅ below). The rest of this README and [`documentation/`](documentation/README.md) describes the API Samsa is being built toward, tagged with the release that ships it. Until `v1.0.0`, minor versions may contain breaking changes.

> *Gregor Samsa woke up one morning transformed. This is Kafka, transformed into a Laravel package.*

## Why Samsa?

You want an **event log** — durable, ordered, replayable, shared by several independent consumers — but:

- Kafka is a lot of infrastructure to run for one Laravel app,
- the Laravel Kafka packages need the `ext-rdkafka` C extension, and
- Laravel's queues delete a job once it runs, so there's nothing to replay and only one consumer per job.

Samsa gives you the Kafka model on storage you already have:

```php
// Producer: same key → same partition → strict order
Samsa::produce('orders', ['id' => 42, 'total' => 99.90], key: 'order-42');
```

```php
// Consumer: every group gets every record, at its own pace
final class ChargeOrder implements Handler
{
    public function handle(Record $record): void
    {
        Payment::charge($record->payload['id'], $record->payload['total']);
    }
}
```

```bash
php artisan samsa:consume billing orders     # the billing group
php artisan samsa:consume emails  orders     # an independent group reading the same log
```

## Features

| Feature | Since | Status |
|---|---|---|
| Topics with a fixed number of partitions, ordering per key | `v0.1` | ✅ |
| **Database driver** (MySQL/MariaDB, PostgreSQL, SQLite) — produce inside your own DB transaction | `v0.1` | ✅ |
| In-memory driver and the `StorageDriverContract` test suite | `v0.1` | ✅ |
| Consumer groups with per-partition offsets, replay and seek | `v0.2` | ✅ |
| Multiple workers with automatic partition rebalancing and crash takeover | `v0.2` | ✅ |
| Retry with backoff, dead letter queue, `samsa:failed:retry` | `v0.2` | ✅ |
| Lag reporting (`samsa:status`), retention (`samsa:prune`), producer dedup, `OnlyOnce` | `v0.2` | ✅ |
| `Samsa::fake()` and test assertions | `v0.3` | |
| **Laravel queue driver** — `QUEUE_CONNECTION=samsa`, no job changes | `v0.3` | |
| Event bridge — `#[PublishTo('orders', key: 'order_id')]` on any event | `v0.3` | |
| `siberfx/message-bus` transport (`'driver' => 'samsa'`) | `v0.3` | |
| **NATS JetStream driver** — closest to Kafka semantics, high throughput | `v0.4` | |
| Redis Streams driver | `v0.5` | |

## Requirements

- PHP 8.2+ (8.3+ for Laravel 13)
- Laravel 12 or 13
- One of: MySQL 8+, PostgreSQL 13+, SQLite 3.35+ — or a NATS 2.10+ server — or Redis 7+

## Installation

```bash
composer require siberfx/samsa

php artisan vendor:publish --tag=samsa-config
php artisan vendor:publish --tag=samsa-migrations
php artisan migrate

php artisan samsa:topic:create orders --partitions=8
```

Continue with the **[Getting started guide](documentation/getting-started.md)**. It takes about 10 minutes from install to your first consumed record.

## At a glance

```php
use Siberfx\Samsa\Facades\Samsa;

// Produce
Samsa::produce('orders', $payload, key: $order->id);

Samsa::topic('orders')
    ->key($order->id)
    ->withHeaders(['tenant' => $tenant->id])
    ->dedupKey("order-placed-{$order->id}")
    ->produce($payload);

// Produce atomically with your business data (Database driver)
DB::transaction(function () use ($order) {
    $order->save();
    Samsa::produce('orders', $order->toArray(), key: $order->id);
});

// Subscribe a handler (or use config/samsa.php)
Samsa::subscribe('billing', 'orders', ChargeOrder::class);

// Forward domain events automatically
#[PublishTo('orders', key: 'orderId')]
final class OrderPlaced
{
    public function __construct(public string $orderId, public float $total) {}
}
```

```bash
php artisan samsa:consume billing orders --batch=100
php artisan samsa:status --group=billing
php artisan samsa:seek billing orders --to=time:2026-10-01T00:00:00Z   # replay
php artisan samsa:failed
php artisan samsa:failed:retry --all
```

## What Samsa is not

Samsa is honest about its scope:

- It does **not** replicate Kafka's distributed, multi-broker, replicated cluster. If you need that, run Kafka.
- Delivery is **at-least-once**. Handlers must be idempotent — see [Idempotency](documentation/idempotency.md).
- There are no transactions across topics and no exactly-once semantics.
- It is **not** a drop-in Kafka client. It borrows Kafka's model, not its wire protocol.

See [When not to use Samsa](documentation/faq.md#when-should-i-not-use-samsa).

## Documentation

| | |
|---|---|
| **Start here** | [Getting started](documentation/getting-started.md) · [Concepts](documentation/concepts.md) · [Configuration](documentation/configuration.md) |
| **Guides** | [Producing](documentation/producing.md) · [Consuming](documentation/consuming.md) · [Queue driver](documentation/queue-driver.md) · [Event bridge](documentation/event-bridge.md) · [message-bus transport](documentation/message-bus-integration.md) · [Idempotency](documentation/idempotency.md) |
| **Drivers** | [Database](documentation/drivers/database.md) · [NATS JetStream](documentation/drivers/nats.md) · [Redis Streams](documentation/drivers/redis-streams.md) |
| **Running it** | [Operations & commands](documentation/operations.md) · [Testing](documentation/testing.md) · [FAQ](documentation/faq.md) |

## License

MIT © siberfx
