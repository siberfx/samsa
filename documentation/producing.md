# Producing Records

- [Basic produce](#basic-produce)
- [The fluent builder](#the-fluent-builder)
- [Keys](#keys)
- [Headers](#headers)
- [Event types](#event-types)
- [Batches](#batches)
- [Deduplication](#deduplication)
- [Producing inside a database transaction](#producing-inside-a-database-transaction)
- [Producing after a transaction commits](#producing-after-a-transaction-commits)
- [Guaranteed delivery to NATS or Redis: the relay pattern](#guaranteed-delivery-to-nats-or-redis-the-relay-pattern)
- [Choosing a partition count](#choosing-a-partition-count)
- [Payload design](#payload-design)
- [Errors](#errors)
- [Producer events](#producer-events)

## Basic produce

```php
use Siberfx\Samsa\Facades\Samsa;

$meta = Samsa::produce(
    topic:   'orders',
    payload: ['order_id' => 42, 'status' => 'placed'],
    key:     '42',
    headers: ['tenant' => 'acme'],
);
```

`produce()` returns a `RecordMetadata`:

| Property | Example | |
|---|---|---|
| `topic` | `orders` | |
| `partition` | `5` | chosen by the partitioner |
| `offset` | `"1287"` | opaque string, unique within the partition |

The record is durable when `produce()` returns. A failure throws (see [Errors](#errors)).

## The fluent builder

The builder reads better when you set several options:

```php
Samsa::topic('orders')
    ->key($order->id)                       // int|string|null, cast to string
    ->withHeaders(['tenant' => $tenant->id])
    ->eventType(OrderPlaced::class)
    ->dedupKey("order-placed-{$order->id}")
    ->produce($order->toEventPayload());
```

Use a non-default driver with `Samsa::driver('nats')->topic('orders')->...`.

## Keys

The key decides the partition, and so the ordering:

- **Same key → same partition → strict order** for that key.
- **`null` key → round-robin**, with no ordering guarantee and the most even load.

```php
Samsa::produce('orders', $placed,  key: "order-{$id}");
Samsa::produce('orders', $paid,    key: "order-{$id}");  // always after $placed
Samsa::produce('orders', $shipped, key: "order-{$id}");  // always after $paid
```

Keys are strings. Integers are cast. Use stable identifiers (IDs, UUIDs), never values that change, such as a status or a timestamp.

## Headers

Headers carry metadata that isn't part of the business payload. Samsa always adds:

| Header | Value |
|---|---|
| `message_id` | A ULID, unique per record. Use it for consumer-side dedup. |
| `produced_at` | ISO-8601 timestamp with microseconds |
| `producer` | `config('app.name')` |

Add your own (`tenant`, `trace_id`, `schema_version`, `causation_id`…). Header values must be scalars.

```php
Samsa::produce('orders', $payload, key: $id, headers: [
    'trace_id'       => request()->header('X-Trace-Id'),
    'schema_version' => 2,
]);
```

## Event types

Set an event type to tell consumers what kind of record it is without inspecting the payload. It's stored in its own column (Database driver) or header (`samsa-event-type`), so it can be filtered cheaply:

```php
Samsa::topic('orders')->eventType('order.placed')->key($id)->produce($payload);
```

```php
public function handle(Record $record): void
{
    match ($record->eventType) {
        'order.placed'  => $this->onPlaced($record),
        'order.shipped' => $this->onShipped($record),
        default         => null, // ignore unknown types: forward compatibility
    };
}
```

Records produced via the [event bridge](event-bridge.md) get the event's class name as their type.

## Batches

For bulk writes, `produceBatch()` groups records by partition and writes each group in one round trip:

```php
use Siberfx\Samsa\Data\OutgoingRecord;

$metas = Samsa::produceBatch('page-views', $views->map(fn ($v) => new OutgoingRecord(
    key:     null,
    payload: $v->toArray(),
))->all());
```

- Order is preserved **within each key**.
- On the Database driver, a batch to one partition is atomic. A batch spanning partitions is atomic only if you wrap it in `DB::transaction()`.
- Keep batches to a few thousand records.

## Deduplication

At-least-once systems need a way to avoid writing the same fact twice, for example when a controller is retried. Give the record a **dedup key**:

```php
Samsa::topic('payments')
    ->key($payment->order_id)
    ->dedupKey("payment-captured-{$payment->id}")
    ->produce($payload);
```

If a record with the same dedup key already exists in the topic, nothing is written, and the **existing** record's metadata is returned.

| Driver | Dedup scope |
|---|---|
| Database | Forever, per topic (until retention removes the original) |
| NATS | JetStream's duplicate window (default 2 minutes, configurable per stream) |
| Redis | A TTL key (default 24 hours) |

Dedup on produce does **not** remove the need for idempotent handlers. See [Idempotency](idempotency.md).

## Producing inside a database transaction

With the **Database driver** (on the same connection as your models), a produce inside a transaction commits or rolls back together with your data:

```php
DB::transaction(function () use ($order) {
    $order->status = 'paid';
    $order->save();

    Samsa::produce('orders', ['order_id' => $order->id, 'status' => 'paid'], key: $order->id);
});
// Either both happened, or neither did.
```

This removes the classic dual-write problem ("the row was saved but the event was never published"), and you don't need a separate outbox table.

> Producing locks the partition's offset counter until the transaction ends. Keep transactions that produce **short**. A slow transaction blocks other producers writing to that partition.

With NATS or Redis there's no shared transaction. Use [after-commit producing](#producing-after-a-transaction-commits), or the [relay pattern](#guaranteed-delivery-to-nats-or-redis-the-relay-pattern) when you can't afford to lose a record.

## Producing after a transaction commits

```php
DB::transaction(function () use ($order) {
    $order->save();
    Samsa::afterCommit()->produce('orders', $order->toArray(), key: $order->id);
});
```

The record is produced only if the transaction commits, and is dropped if it rolls back. If the process dies between the commit and the produce, the record is lost. Use the relay pattern below when you can't accept that.

## Guaranteed delivery to NATS or Redis: the relay pattern

To get transactional guarantees *and* a NATS or Redis log, produce to a Database-driver topic inside the transaction (it acts as the outbox), then let a small consumer group forward it:

```php
// 1. Inside the transaction: atomic with your data
DB::transaction(function () use ($order) {
    $order->save();
    Samsa::driver('database')->produce('orders-outbox', $order->toArray(), key: $order->id);
});

// 2. A relay group forwards to NATS; dedup by message_id makes redelivery harmless
Samsa::subscribe('relay', 'orders-outbox', function (Record $record) {
    Samsa::driver('nats')->topic('orders')
        ->key($record->key)
        ->withHeaders($record->headers)
        ->dedupKey($record->messageId())
        ->produce($record->payload);
});
```

```bash
php artisan samsa:consume relay orders-outbox --driver=database
```

Give the outbox topic a short retention (e.g. one day). It only needs to hold records until the relay forwards them.

## Choosing a partition count

The partition count is the **maximum parallelism per group**: 8 partitions means at most 8 workers of `billing` doing useful work at once.

Guidelines:

- Estimate peak records/second ÷ records/second one worker handles, then **double it**.
- Small apps: 4–8. Busy topics: 16–64. Partitions are cheap on every driver, but each idle partition costs one poll per worker loop on the Database driver.
- You can **increase** the count later with `samsa:topic:alter orders --partitions=16 --force`, but:
  - keys get remapped, so records for one key can end up in two partitions while the old one drains, and
  - per-key ordering is not guaranteed during that transition.
- You can never **decrease** the count.

If ordering matters, the safe way to change the count is to create a new topic, produce to both, switch consumers, then stop producing to the old one.

## Payload design

- Payloads must be JSON-serializable arrays. Use IDs and scalar values, not Eloquent models or closures.
- Prefer **facts** ("order 42 was paid, 99.90 EUR") over commands ("charge order 42").
- Include everything a consumer needs to act **without reading your database**, because the row may have changed since the record was written.
- Version your payloads (`schema_version` header) and only ever *add* fields. Consumers should ignore fields they don't know.
- Keep records small (under ~100 KB). Store large blobs elsewhere and reference them.

## Errors

| Exception | When |
|---|---|
| `TopicNotFoundException` | The topic doesn't exist and `auto_create_topics` is off |
| `SerializationException` | The payload or headers can't be encoded as JSON |
| `InvalidTopicException` | The topic name is invalid, or the partition doesn't exist |
| `CrossConnectionTransactionException` | A transaction is open on another connection than Samsa's (see [Database driver](drivers/database.md#transactional-produce)) |
| Backend exceptions | The Database driver lets Laravel's `QueryException` through unchanged (connection lost, deadlock, …). Network drivers (`v1.2+`) wrap failures in `DriverException` |

Producing is **not** retried automatically. Wrap it in Laravel's `retry()` if you want that:

```php
retry(3, fn () => Samsa::produce('orders', $payload, key: $id), sleepMilliseconds: 200);
```

## Producer events

| Event | Payload |
|---|---|
| `Siberfx\Samsa\Events\RecordProduced` | `RecordMetadata $metadata`, `OutgoingRecord $record` |

Use it for metrics or audit logs. It fires synchronously after the write, inside the transaction if there is one.
