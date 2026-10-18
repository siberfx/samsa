# siberfx/message-bus Integration


[`siberfx/message-bus`](https://github.com/siberfx/message-bus) is a Symfony Messenger-style bus for Laravel: messages, handlers, middleware, transports, retries and a failure transport. Samsa plugs into it as a **transport**. Messages routed to it are written to a Samsa topic and consumed through a consumer group, so you get per-key ordering, replay and multiple independent readers behind the bus API you already use.

- [Requirements](#requirements)
- [Setup](#setup)
- [Routing messages to Samsa](#routing-messages-to-samsa)
- [Ordering: partition keys](#ordering-partition-keys)
- [Consuming](#consuming)
- [How the transport contract maps to the log](#how-the-transport-contract-maps-to-the-log)
- [Retries and the failure transport](#retries-and-the-failure-transport)
- [Atomic dispatch (transactional outbox)](#atomic-dispatch-transactional-outbox)
- [Reading bus messages from other groups](#reading-bus-messages-from-other-groups)
- [Limitations](#limitations)

## Requirements

`siberfx/message-bus` (`^0.1`) has the same requirements as Samsa: PHP 8.3+ and Laravel 13.

```bash
composer require siberfx/samsa siberfx/message-bus
```

When both packages are installed, Samsa registers the `samsa` transport driver automatically:

```php
// done for you by SamsaServiceProvider
MessageBus::extendTransport('samsa', fn (array $config, string $name, $app) => /* SamsaTransport */);
```

## Setup

Add a transport to `config/message-bus.php`:

```php
'transports' => [
    // ...

    'events' => [
        'driver' => 'samsa',
        'topic'  => 'bus-events',      // Samsa topic the messages are written to
        'group'  => 'message-bus',     // Samsa consumer group used by message-bus:consume
        'samsa_driver' => null,        // null = config('samsa.default')
        'batch_size' => 10,            // records handed to the worker per get()
        'lease_seconds' => 90,         // a message must be handled within its lease
        'retry' => [
            'max_retries' => 3,
            'delay' => 1000,
        ],
        'failure_transport' => 'failed',
    ],
],
```

Create the topic:

```bash
php artisan samsa:topic:create bus-events --partitions=8
```

## Routing messages to Samsa

Use message-bus's normal routing, either in config:

```php
'routing' => [
    \App\Messages\OrderPlaced::class => 'events',
],
```

or on the message:

```php
use Siberfx\MessageBus\Attribute\AsMessage;

#[AsMessage(alias: 'order.placed', transport: 'events')]
final class OrderPlaced
{
    public function __construct(public string $orderId, public float $total) {}
}
```

Dispatching doesn't change:

```php
MessageBus::dispatch(new OrderPlaced($order->id, $order->total));
```

The message is serialized with the transport's configured message-bus serializer, its stamps are carried in the record headers, and it is produced to `bus-events`.

## Ordering: partition keys

Without a key, messages are spread round-robin. To keep the messages for one entity in order, give them a key. You can do it on the message:

```php
use Siberfx\Samsa\Bus\HasPartitionKey;

#[AsMessage(alias: 'order.placed', transport: 'events')]
final class OrderPlaced implements HasPartitionKey
{
    public function __construct(public string $orderId, public float $total) {}

    public function partitionKey(): string
    {
        return "order-{$this->orderId}";
    }
}
```

or per dispatch, with a stamp:

```php
use Siberfx\Samsa\Bus\PartitionKeyStamp;

MessageBus::dispatch(new OrderPlaced(...), [new PartitionKeyStamp("order-{$id}")]);
```

## Consuming

Use message-bus's own worker:

```bash
php artisan message-bus:consume events
```

Each worker process joins the transport's Samsa group (`message-bus`) and gets a fair share of the topic's partitions, exactly like `samsa:consume`. Run several for parallelism (up to the partition count) and use Supervisor as usual. Graceful shutdown, lease expiry on crash and fencing all behave as described in [Consuming](consuming.md#scaling-out-and-rebalancing).

## How the transport contract maps to the log

| `TransportInterface` | Samsa |
|---|---|
| `send(Envelope)` | `produce()` to the topic; adds a `TransportMessageIdStamp` with `"{partition}:{offset}"` |
| `get()` | Reads the next batch from the partitions this worker leases |
| `ack(Envelope)` | Marks the record done; the offset is committed once every earlier record in the partition is done |
| `reject(Envelope)` | Marks the record done (moves past it). message-bus has already re-sent it for retry, or sent it to the failure transport |

Commits only move forward over a contiguous run of finished records, so an envelope that is still in flight never gets skipped, even if later ones finish first.

## Retries and the failure transport

message-bus owns retries for this transport, the same way Laravel owns retries in the [queue driver](queue-driver.md):

1. A handler throws → message-bus's retry strategy re-sends the envelope with a `RedeliveryStamp` and a `DelayStamp`.
2. Samsa puts the delayed copy in its [delayed store](queue-driver.md#delayed-jobs), and `samsa:release-delayed` produces it when due.
3. The original record is rejected (committed past).
4. After `max_retries`, message-bus sends the envelope to the `failure_transport`. Use `message-bus:failed:*` commands to inspect and retry.

Samsa's own `retry.*` settings and dead-letter store are **not** used for bus messages.

> **Ordering note:** a retried message is re-produced at the **end** of its partition, so later messages with the same key can overtake it. If that's not acceptable for a message type, set `max_retries` to `0` on its transport and handle the failure in place, or consume that topic with a native Samsa handler (which [retries in place](consuming.md#retries)).

## Atomic dispatch (transactional outbox)

With Samsa's **Database driver** on the same connection as your models, a dispatch inside a transaction is atomic with your data. The record is the outbox:

```php
DB::transaction(function () use ($order) {
    $order->save();
    MessageBus::dispatch(new OrderPlaced($order->id, $order->total)); // same transaction
});
```

message-bus's `DatabaseTransactionMiddleware` composes with this. Handlers that dispatch further messages inside their transaction get the same guarantee.

With the NATS or Redis drivers there's no shared transaction, so dispatch happens immediately.

## Reading bus messages from other groups

Records written by the transport are ordinary Samsa records, so any other group can read them with a native handler, for analytics, auditing or a separate service:

```php
Samsa::subscribe('audit', 'bus-events', function (Record $record) {
    // $record->eventType  === 'order.placed' (the #[AsMessage] alias, or the class name)
    // $record->payload    === the serialized message body
    // $record->headers    includes the message-bus stamps under 'bus-stamps'
});
```

Use a JSON serializer on the transport if non-PHP consumers will read the topic.

## Limitations

- Laravel 13 / PHP 8.3+ only (message-bus's requirement).
- `message-bus:stats` reports the transport's lag through `MessageCountAwareInterface`.
- `ListableReceiverInterface` (listing pending messages) is supported read-only. Removing a single pending message isn't possible in a log; seek or let it be consumed instead.
