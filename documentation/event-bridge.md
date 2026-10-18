# Event Bridge


The event bridge turns ordinary Laravel events into Samsa records, so the rest of your system can consume them from the log.

- [`#[PublishTo]`](#publishto)
- [Choosing the key](#choosing-the-key)
- [What gets stored](#what-gets-stored)
- [Customising the payload](#customising-the-payload)
- [`Samsa::publish()`](#samsapublish)
- [Transactions](#transactions)
- [Forwarding broadcast events](#forwarding-broadcast-events)
- [Consuming bridged events](#consuming-bridged-events)
- [Performance](#performance)

## `#[PublishTo]`

Mark an event class with the attribute:

```php
use Siberfx\Samsa\Attributes\PublishTo;

#[PublishTo('orders', key: 'orderId')]
final class OrderPlaced
{
    use Dispatchable;

    public function __construct(
        public string $orderId,
        public float  $total,
        public string $currency,
    ) {}
}
```

Dispatch it normally:

```php
OrderPlaced::dispatch($order->id, $order->total, 'EUR');
// or: event(new OrderPlaced(...));
```

Your in-app listeners still run, and the event is **also** produced to the `orders` topic.

Attribute arguments:

| Argument | Default | Description |
|---|---|---|
| `topic` | — | Target topic (required) |
| `key` | `null` | Name of a public property (or method) whose value is the record key |
| `eventType` | the event's class name | Value of `$record->eventType`. Set a stable string (e.g. `order.placed`) so renaming the class doesn't break consumers |
| `driver` | default driver | Produce through another driver |
| `afterCommit` | `true` | Produce only after the surrounding DB transaction commits (see [Transactions](#transactions)) |

The attribute is repeatable, so an event can go to several topics:

```php
#[PublishTo('orders', key: 'orderId', eventType: 'order.placed')]
#[PublishTo('analytics', eventType: 'order.placed')]
final class OrderPlaced { /* ... */ }
```

## Choosing the key

`key:` names a property or a zero-argument method:

```php
#[PublishTo('accounts', key: 'partitionKey')]
final class BalanceChanged
{
    public function __construct(public Account $account, public int $delta) {}

    public function partitionKey(): string
    {
        return "account-{$this->account->id}";
    }
}
```

All events with the same key are ordered relative to each other. See [Concepts → Keys and ordering](concepts.md#keys-and-ordering).

## What gets stored

By default the payload is the event's **public properties**, serialized:

| Property value | Stored as |
|---|---|
| Scalars, arrays, `null` | As is |
| `BackedEnum` | Its value |
| `DateTimeInterface` | ISO-8601 string |
| Eloquent model | `['type' => Model::class, 'id' => $key]` (the identifier only, never the whole model) |
| `Arrayable` / `JsonSerializable` | `toArray()` / `jsonSerialize()` |
| Anything else | `SerializationException` |

## Customising the payload

Implement `ToSamsaPayload` for full control:

```php
use Siberfx\Samsa\Contracts\ToSamsaPayload;

#[PublishTo('orders', key: 'orderId', eventType: 'order.placed')]
final class OrderPlaced implements ToSamsaPayload
{
    public function __construct(public Order $order) {}

    public function toSamsaPayload(): array
    {
        return [
            'order_id' => $this->order->id,
            'total'    => $this->order->total,
            'items'    => $this->order->items->map->only('sku', 'qty')->all(),
        ];
    }

    public function samsaHeaders(): array   // optional
    {
        return ['tenant' => $this->order->tenant_id];
    }
}
```

## `Samsa::publish()`

Publish an event object without dispatching it through Laravel's event system:

```php
Samsa::publish(new OrderPlaced($order));                     // uses the attribute
Samsa::publish(new OrderPlaced($order), topic: 'orders-eu'); // overrides the topic
Samsa::publish($anyObject, topic: 'misc', key: 'abc');       // works without an attribute
```

## Transactions

Bridged events default to `afterCommit: true`. When the event is dispatched inside `DB::transaction()`, the record is produced **after** the commit, and not at all if the transaction rolls back.

With the **Database driver** you can produce atomically instead, as part of the transaction:

```php
#[PublishTo('orders', key: 'orderId', afterCommit: false)]
```

The record then commits or rolls back together with your data, with no outbox needed. With NATS or Redis, `afterCommit: false` simply produces immediately. For guaranteed delivery to those drivers, use the [database relay pattern](producing.md#guaranteed-delivery-to-nats-or-redis-the-relay-pattern).

## Forwarding broadcast events

Optionally, every `ShouldBroadcast` event can be mirrored to the log, which is handy for an audit trail of real-time events:

```php
// config/samsa.php
'bridge' => [
    'enabled'                  => true,
    'forward_broadcast_events' => true,
    'broadcast_topic'          => 'broadcasts',
],
```

The key is the first broadcast channel name, so events on one channel stay ordered. The payload is `broadcastWith()` (or the public properties). The event type is `broadcastAs()` (or the class name).

## Consuming bridged events

Bridged events are ordinary records:

```php
final class ProjectOrderStats implements Handler
{
    public function handle(Record $record): void
    {
        if ($record->eventType !== 'order.placed') {
            return;
        }

        DailyStats::increment($record->createdAt->toDateString(), $record->payload['total']);
    }
}
```

Consumers don't need the original event class, and can live in a completely different application that reads the same database or NATS server.

## Performance

- The bridge only reflects on an event class once and caches the result. Events without the attribute cost one cached lookup.
- Producing is **synchronous** in the request. On the Database driver that's one insert in your transaction. If an event fires thousands of times per request, collect the payloads and use `Samsa::produceBatch()` instead.
- Disable the bridge entirely with `bridge.enabled = false`.
