# Idempotency

Samsa delivers every record **at least once**. In normal operation that means exactly once, but in some situations a record is delivered again:

| Situation | Why it redelivers |
|---|---|
| A worker crashes after handling records but before committing | The next owner resumes from the last commit |
| A worker loses its lease mid-batch (long pause, rebalance) | Its commit is refused (fencing); the new owner re-reads |
| `samsa:seek` to an earlier position | Deliberate replay |
| `samsa:failed:retry` | The record is re-produced, and **every** group sees it again |
| The relay pattern or the producer retries a produce | The same fact may be written twice (unless you use a dedup key) |

A handler is **idempotent** when running it twice for the same record has the same effect as running it once. Every Samsa handler should be.

- [Pattern 1: naturally idempotent writes](#pattern-1-naturally-idempotent-writes)
- [Pattern 2: a processed-messages table](#pattern-2-a-processed-messages-table)
- [Pattern 3: version or offset guards](#pattern-3-version-or-offset-guards)
- [Pattern 4: idempotency keys for external APIs](#pattern-4-idempotency-keys-for-external-apis)
- [Producer-side dedup](#producer-side-dedup)
- [Shrinking the redelivery window](#shrinking-the-redelivery-window)
- [Checklist](#checklist)

## Pattern 1: naturally idempotent writes

Prefer operations that are idempotent by nature, such as **set** rather than **add**, and **upsert** rather than **insert**:

```php
// ❌ Not idempotent: a redelivery double-counts
$product->increment('stock', $record->payload['delta']);

// ✅ Idempotent: the record carries the resulting value
$product->update(['stock' => $record->payload['stock_after']]);

// ✅ Idempotent: upsert keyed by a natural identifier
OrderSummary::upsert(
    [['order_id' => $record->payload['order_id'], 'status' => $record->payload['status']]],
    uniqueBy: ['order_id'],
    update: ['status'],
);
```

This is why [producing facts with full state](producing.md#payload-design) pays off.

## Pattern 2: a processed-messages table

When the side effect isn't naturally idempotent, record that you've handled the message **in the same transaction** as the side effect. Samsa's migrations include a table for this, `samsa_processed`, with the primary key `(scope, message_id)`, and a helper that uses it:

```php
use Siberfx\Samsa\Idempotency\OnlyOnce;

final class CreditLoyaltyPoints implements Handler
{
    public function __construct(private OnlyOnce $onlyOnce) {}

    public function handle(Record $record): void
    {
        $this->onlyOnce->run('loyalty', $record, function () use ($record) {
            Customer::find($record->payload['customer_id'])
                ->increment('points', $record->payload['points']);
        });
        // Second delivery: the callback is skipped, and the handler returns normally.
    }
}
```

`OnlyOnce` inserts `(scope, message_id)` (here the scope is `loyalty`) and runs the callback inside one DB transaction on your default connection, so it protects side effects in that database. If the insert hits the primary key, the message was already processed and the callback is skipped. Prune old rows with `samsa:prune --processed-older-than=30d`.

Use `message_id` rather than `topic/partition/offset` as the dedup identity. It survives `samsa:failed:retry` and the relay pattern, which both re-produce the record at a new offset.

## Pattern 3: version or offset guards

For read models updated by one group, store the last applied position and ignore older ones:

```php
public function handle(Record $record): void
{
    $summary = OrderSummary::firstOrNew(['order_id' => $record->payload['order_id']]);

    if ($summary->version >= $record->payload['version']) {
        return; // already applied (or newer state already present)
    }

    $summary->fill([...$record->payload])->save();
}
```

Producers include a monotonically increasing `version` per entity (e.g. the model's `updated_at` in microseconds, or an explicit counter). Because records for one key are ordered, the guard rarely triggers, but it makes replays safe.

## Pattern 4: idempotency keys for external APIs

Payment providers, email APIs and many SaaS APIs accept an idempotency key. Pass the record's `message_id`:

```php
$this->stripe->charges->create([
    'amount'   => $record->payload['amount'],
    'currency' => 'eur',
    'customer' => $record->payload['customer_id'],
], ['idempotency_key' => $record->messageId()]);
```

For APIs without that feature, combine Pattern 2 with a "sent" marker, and accept that a crash *between* the API call and the marker can still cause one duplicate. This is the fundamental limit of at-least-once delivery.

## Producer-side dedup

Duplicates can also come from the producing side, for example a controller retried by a load balancer. Use a [dedup key](producing.md#deduplication):

```php
Samsa::topic('payments')->key($orderId)->dedupKey("captured-{$paymentId}")->produce($payload);
```

Producer dedup and handler idempotency solve different problems. You usually want both.

## Shrinking the redelivery window

You can't remove redelivery, but you can make it smaller:

- `--commit=record` (or `consumer.commit = record`): after a crash, at most one record per partition is redelivered instead of a whole batch.
- A smaller `--batch`: less work is in flight per partition.
- Handle `SIGTERM` gracefully, which Samsa does for you, and give Supervisor enough `stopwaitsecs`, so deploys never cause redeliveries.

## Checklist

- [ ] Does every handler survive running twice for the same record?
- [ ] Do handlers that call external APIs pass `message_id` as an idempotency key?
- [ ] Do counters and balances use `OnlyOnce` or carry the resulting value?
- [ ] Do producers that can be retried set a `dedupKey`?
- [ ] Is there a test that delivers the same record twice? See [Testing → Redelivery](testing.md#testing-redelivery).
