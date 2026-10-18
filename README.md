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

<p align="center">
  <a href="https://github.com/siberfx/samsa/actions/workflows/tests.yml"><img src="https://github.com/siberfx/samsa/actions/workflows/tests.yml/badge.svg" alt="Tests"></a>
  <a href="https://packagist.org/packages/siberfx/samsa"><img src="https://img.shields.io/packagist/v/siberfx/samsa" alt="Latest version"></a>
  <a href="https://packagist.org/packages/siberfx/samsa"><img src="https://img.shields.io/packagist/php-v/siberfx/samsa" alt="PHP version"></a>
  <a href="LICENSE.md"><img src="https://img.shields.io/packagist/l/siberfx/samsa" alt="License"></a>
</p>

> **Shipped:** the Database and memory drivers, producers, consumer groups, workers, retries, dead letters, the operations commands (1.0), the Laravel queue driver, event bridge, `Samsa::fake()` and the `siberfx/message-bus` transport (1.1), and the NATS JetStream driver (1.2). Everything is tested on SQLite, MySQL/MariaDB, PostgreSQL and NATS, including multi-process tests. The Redis Streams driver is **planned** for 1.3, and its documentation page is marked as such.

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

| Feature | Status |
|---|---|
| Topics with a fixed number of partitions, ordering per key | ✅ 1.0 |
| **Database driver** (MySQL/MariaDB, PostgreSQL, SQLite) — produce inside your own DB transaction | ✅ 1.0 |
| In-memory driver and the `StorageDriverContract` test suite for custom drivers | ✅ 1.0 |
| Consumer groups with per-partition offsets, replay and seek | ✅ 1.0 |
| Multiple workers with automatic partition rebalancing and crash takeover | ✅ 1.0 |
| Retry with backoff, dead letter queue, `samsa:failed:retry` | ✅ 1.0 |
| Lag reporting (`samsa:status`), retention (`samsa:prune`), producer dedup, `OnlyOnce` | ✅ 1.0 |
| `Samsa::fake()` and test assertions | ✅ 1.1 |
| **Laravel queue driver** — `QUEUE_CONNECTION=samsa`, no job changes | ✅ 1.1 |
| Event bridge — `#[PublishTo('orders', key: 'order_id')]` on any event | ✅ 1.1 |
| `siberfx/message-bus` transport (`'driver' => 'samsa'`) | ✅ 1.1 |
| **NATS JetStream driver** — off-database, replicated, high throughput | ✅ 1.2 |
| Redis Streams driver | Planned — 1.3 |

## Requirements

- PHP 8.2+ (8.3+ for Laravel 13)
- Laravel 12 or 13
- MySQL 8+ / MariaDB 10.6+, PostgreSQL 13+ or SQLite 3.35+, or a NATS 2.10+ server with JetStream (`composer require basis-company/nats`). A Redis driver is planned
- `ext-pcntl` is recommended so workers shut down gracefully on `SIGTERM` (not available on Windows)

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

// Retry a flaky handler 5 times before it is dead-lettered
#[Retry(maxAttempts: 5, backoffMs: [200, 1000, 5000])]
final class ChargeOrder implements Handler
{
    public function handle(Record $record): void { /* ... */ }
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
