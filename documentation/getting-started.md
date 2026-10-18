# Getting Started

This guide takes you from `composer require` to a record that is produced, consumed, retried and replayed. It uses the **Database driver**, so you need nothing beyond your app's existing database.

- [1. Install](#1-install)
- [2. Create a topic](#2-create-a-topic)
- [3. Produce a record](#3-produce-a-record)
- [4. Write a handler](#4-write-a-handler)
- [5. Run a consumer](#5-run-a-consumer)
- [6. Add a second consumer group](#6-add-a-second-consumer-group)
- [7. Replay](#7-replay)
- [8. See a failure land in the dead letter queue](#8-see-a-failure-land-in-the-dead-letter-queue)
- [Next steps](#next-steps)

## 1. Install

```bash
composer require siberfx/samsa
```

The service provider and the `Samsa` facade are auto-discovered. Publish the config and migrations, then migrate:

```bash
php artisan vendor:publish --tag=samsa-config
php artisan vendor:publish --tag=samsa-migrations
php artisan migrate
```

This creates the `samsa_*` tables (see [Database driver](drivers/database.md#tables)) and `config/samsa.php`.

> **Using a separate database for the log?** Set `SAMSA_DB_CONNECTION=events` in `.env` before migrating. See [Configuration](configuration.md#drivers).

## 2. Create a topic

A topic is a named log. You choose its number of **partitions** up front. More partitions means more workers can process it in parallel. Changing the count later remaps keys (see [Choosing a partition count](producing.md#choosing-a-partition-count)), so pick generously.

```bash
php artisan samsa:topic:create orders --partitions=8
```

Or declare it in `config/samsa.php` so every environment gets the same topology:

```php
'topics' => [
    'orders' => ['partitions' => 8],
],
```

```bash
php artisan samsa:topic:create --from-config
```

## 3. Produce a record

```php
use Siberfx\Samsa\Facades\Samsa;

$meta = Samsa::produce('orders', [
    'order_id' => $order->id,
    'total'    => $order->total,
], key: (string) $order->id);

$meta->partition; // e.g. 5
$meta->offset;    // e.g. "0"
```

The **key** decides the partition. All records with key `"42"` land in the same partition, in the order you produced them. That is Samsa's ordering guarantee — **per key**, not per topic.

Try it from the command line too:

```bash
php artisan samsa:produce orders '{"order_id": 42, "total": 99.9}' --key=42
```

## 4. Write a handler

A handler receives one `Record` at a time:

```php
namespace App\Samsa\Handlers;

use Siberfx\Samsa\Contracts\Handler;
use Siberfx\Samsa\Data\Record;

final class ChargeOrder implements Handler
{
    public function __construct(private PaymentGateway $payments) {}   // resolved from the container

    public function handle(Record $record): void
    {
        $this->payments->charge(
            orderId: $record->payload['order_id'],
            amount:  $record->payload['total'],
        );
    }
}
```

Subscribe it to the topic under a **consumer group** name. Either use config:

```php
// config/samsa.php
'subscriptions' => [
    'billing' => [
        'orders' => \App\Samsa\Handlers\ChargeOrder::class,
    ],
],
```

or a service provider's `boot()` method:

```php
Samsa::subscribe('billing', 'orders', ChargeOrder::class);
```

## 5. Run a consumer

```bash
php artisan samsa:consume billing orders
```

The worker:

1. joins group `billing` and claims (leases) partitions of `orders`,
2. reads records after the group's committed offset,
3. calls `ChargeOrder::handle()` for each,
4. commits the offset after each batch,
5. polls for new records when it's caught up.

Stop it with `Ctrl+C`. It finishes the current record, commits, and releases its partitions. Start it again and it continues exactly where it stopped.

Check progress:

```bash
php artisan samsa:status --group=billing
```

```
 Group    Topic   Partition  Latest  Committed  Lag  Owner
 billing  orders  0          12      12         0    web-1:4821
 billing  orders  1          9       9          0    web-1:4821
 ...
```

Run a second `samsa:consume billing orders` in another terminal. The two workers split the 8 partitions between them. Kill one with `kill -9` and, after the lease expires (30 seconds by default), the other takes its partitions back. See [Consuming](consuming.md#scaling-out-and-rebalancing).

## 6. Add a second consumer group

Groups are independent. A new group reads the **whole** log from the start (or from the end, if you choose), without affecting `billing`:

```php
'subscriptions' => [
    'billing' => ['orders' => ChargeOrder::class],
    'emails'  => ['orders' => SendOrderConfirmation::class],
],
```

```bash
php artisan samsa:consume emails orders
```

This is the main difference from a queue: one record, many independent readers.

## 7. Replay

Fixed a bug in a handler and need to reprocess last week's records?

```bash
php artisan samsa:seek emails orders --to=time:2026-10-11T00:00:00Z
php artisan samsa:consume emails orders
```

Other targets: `--to=earliest`, `--to=latest`, `--to=offset:120 --partition=3`. See [Replay and seek](consuming.md#replay-and-seek).

## 8. See a failure land in the dead letter queue

Throw from a handler:

```php
public function handle(Record $record): void
{
    throw new RuntimeException('Gateway down');
}
```

By default Samsa retries the record **3 times** with backoff (1s, 5s, 15s), then stores it as a **dead letter**, commits its offset and moves on, so one bad record never blocks the partition.

```bash
php artisan samsa:failed
php artisan samsa:failed:retry 9b1c...   # after fixing the cause
```

## Next steps

- Read **[Concepts](concepts.md)**. It covers delivery guarantees, which every handler author should understand.
- Make handlers safe to run twice: **[Idempotency](idempotency.md)**.
- Run workers in production with Supervisor: **[Consuming → Running in production](consuming.md#running-in-production)**.
- Already using Laravel queues? Try the **[Queue driver](queue-driver.md)**.
- Outgrowing the database? Switch to **[NATS JetStream](drivers/nats.md)** without changing your handlers.
