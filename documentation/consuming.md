# Consuming Records

- [Handlers](#handlers)
- [The Record object](#the-record-object)
- [Subscribing](#subscribing)
- [Running a worker](#running-a-worker)
- [Worker options](#worker-options)
- [Scaling out and rebalancing](#scaling-out-and-rebalancing)
- [Commits](#commits)
- [Retries](#retries)
- [Dead letters](#dead-letters)
- [Replay and seek](#replay-and-seek)
- [Batch handlers](#batch-handlers)
- [Consumer events](#consumer-events)
- [Running in production](#running-in-production)
- [Troubleshooting](#troubleshooting)

## Handlers

A handler is a class that implements `Siberfx\Samsa\Contracts\Handler`:

```php
namespace App\Samsa\Handlers;

use Siberfx\Samsa\Contracts\Handler;
use Siberfx\Samsa\Data\Record;

final class SendOrderConfirmation implements Handler
{
    public function __construct(private Mailer $mailer) {}

    public function handle(Record $record): void
    {
        $order = Order::find($record->payload['order_id']);

        if ($order === null) {
            return; // nothing to do — returning normally commits the record
        }

        $this->mailer->to($order->customer)->send(new OrderConfirmed($order));
    }
}
```

- Handlers are resolved **once per worker** from the container, so constructor injection works.
- Returning normally means **success**. Throwing means **failure**, which triggers a retry and then a dead letter.
- A handler **must be idempotent**. See [Idempotency](idempotency.md).
- Don't keep per-record state in handler properties. The same instance handles every record.

Closures work too, which is handy for small apps and tests:

```php
Samsa::subscribe('audit', 'orders', function (Record $record) {
    AuditLog::create(['topic' => $record->topic, 'payload' => $record->payload]);
});
```

## The Record object

`Siberfx\Samsa\Data\Record` is immutable:

| Property | Type | Description |
|---|---|---|
| `topic` | `string` | |
| `partition` | `int` | |
| `offset` | `string` | Opaque, unique within the partition |
| `key` | `?string` | |
| `eventType` | `?string` | See [Producing → Event types](producing.md#event-types) |
| `payload` | `array` | Decoded JSON |
| `headers` | `array` | Includes `message_id`, `produced_at`, `producer` |
| `createdAt` | `CarbonImmutable` | When it was written |
| `deliveryAttempt` | `int` | 1 on the first try, 2+ on retries |

Helpers: `$record->header('trace_id')`, `$record->messageId()`, `$record->isRedelivery()`.

## Subscribing

A subscription binds **group + topic → handler**. One group can subscribe to several topics, and one topic can have many groups.

```php
// config/samsa.php
'subscriptions' => [
    'billing' => [
        'orders'  => ChargeOrder::class,
        'refunds' => IssueRefund::class,
    ],
],
```

```php
// or in a service provider's boot()
Samsa::subscribe('billing', 'orders', ChargeOrder::class);
```

Renaming a group creates a **new** group that starts from `consumer.start_from`. To keep the position, copy the offsets first with `samsa:seek new-group orders --to=group:old-group`.

## Running a worker

```bash
php artisan samsa:consume {group} {topic*}
```

```bash
php artisan samsa:consume billing orders
php artisan samsa:consume billing orders refunds      # several topics, one process
```

What the worker does in a loop:

1. Heartbeats its membership and renews or claims partition leases (fair share).
2. For each owned partition, reads up to `--batch` records after the committed offset.
3. Runs the handler for each record, with retries.
4. Commits (after the batch, or after every record in `record` mode). It does this only if it still holds the lease.
5. Sleeps `poll_interval_ms` if every partition was empty.

Signals:

| Signal | Effect |
|---|---|
| `SIGTERM`, `SIGINT` | Finish the current record, commit, release leases, exit 0 |
| `SIGQUIT` | Same as above, without waiting for the in-flight retry backoff |
| `php artisan samsa:restart` | Every worker exits gracefully after its current batch (like `queue:restart`) |

## Worker options

| Option | Default | Description |
|---|---|---|
| `--batch=` | `consumer.batch_size` | Records per read per partition |
| `--sleep=` | `consumer.poll_interval_ms` | Idle sleep, in ms |
| `--commit=` | `consumer.commit` | `batch` or `record` |
| `--partitions=` | all | Only claim these partitions, e.g. `0,1,2`. Disables fair share |
| `--once` | | Process at most one batch per partition, then exit. Useful in tests and cron |
| `--stop-when-empty` | | Exit when every owned partition is caught up |
| `--max-records=` | | Exit after N records |
| `--max-time=` | | Exit after N seconds |
| `--memory=` | `128` | Exit gracefully above N MB |
| `--driver=` | `default` | Use another driver |

Combine `--max-records`, `--max-time` or `--memory` with Supervisor's auto-restart to recycle workers.

## Scaling out and rebalancing

Run more processes with the **same group** to share the work:

```
8 partitions, 1 worker   →  A: 0-7
add worker B             →  A: 0-3   B: 4-7         (A releases 4-7 on its next loop)
add worker C             →  A: 0-2   B: 3-5   C: 6-7
B crashes                →  A: 0-2 + some of 3-5 after lease_seconds   C: 6-7 + the rest
```

- Parallelism per group is capped at the partition count. Extra workers wait as hot standbys.
- After a crash, the takeover happens after `lease_seconds` (default 30s). Lowering it speeds up takeover, but `lease_seconds` must stay above your slowest record's handling time.
- During a rebalance a partition is never owned by two workers of the same group. A worker that lost its lease can't commit (**fencing**). Its in-flight record will be redelivered to the new owner, which is one more reason handlers must be idempotent.

## Commits

| Mode | Commits | Redelivered after a crash | Writes |
|---|---|---|---|
| `batch` (default) | After each batch | Up to one batch per partition | Few |
| `record` | After each record | At most one record per partition | One per record |

You can also commit manually from a [batch handler](#batch-handlers).

## Retries

When a handler throws, the record is retried **in place**:

```
attempt 1 ✗ → wait 1s → attempt 2 ✗ → wait 5s → attempt 3 ✗ → dead letter → commit → next record
```

Configure globally in `config/samsa.php` (`retry.max_attempts`, `retry.backoff_ms`), or per handler:

```php
use Siberfx\Samsa\Attributes\Retry;

#[Retry(maxAttempts: 5, backoffMs: [200, 1000, 5000, 30000])]
final class ChargeOrder implements Handler { /* ... */ }
```

Skip retries for errors that will never succeed:

```php
use Siberfx\Samsa\Exceptions\UnrecoverableRecordException;

if (! isset($record->payload['order_id'])) {
    throw new UnrecoverableRecordException('Missing order_id');
}
```

Make an exception class unrecoverable everywhere by listing it under `retry.dont_retry`:

```php
'retry' => [
    'max_attempts' => 3,
    'backoff_ms'   => [1000, 5000, 15000],
    'dont_retry'   => [\Illuminate\Validation\ValidationException::class],
],
```

> **Retries block the partition** for this group while they run. That's the price of ordering. If a dependency is down for minutes, it's better to fail fast and retry dead letters later than to back off for minutes in place.

## Dead letters

A record that exhausts its retries is stored with its group, topic, partition, offset, payload, headers, attempt count and the exception. The group's offset then moves past it.

```bash
php artisan samsa:failed                          # list
php artisan samsa:failed --group=billing --topic=orders
php artisan samsa:failed:show 9b1c...             # full payload + stack trace
php artisan samsa:failed:retry 9b1c...            # re-produce to the original topic
php artisan samsa:failed:retry --all --group=billing
php artisan samsa:failed:forget 9b1c...
php artisan samsa:failed:flush --older-than=30d
```

**What does "retry" mean for a log?** The record is **produced again** to its original topic, with the same key, payload and headers, plus a `samsa-retry-of` header. Two things follow:

- **Every group** subscribed to that topic will see it again, not only the group that failed. Handlers in other groups must treat it as a duplicate. The `message_id` header is unchanged, which makes this easy. See [Idempotency](idempotency.md).
- It is placed at the **end** of its partition, after newer records with the same key.

To redeliver to one group only, without re-producing, run the handler directly:

```bash
php artisan samsa:failed:retry 9b1c... --in-place   # runs the group's handler now, in this process
```

Dead-letter storage per driver: the `samsa_failed` table (Database), state entries in the `SAMSA__STATE` stream (NATS) or the `{prefix}{topic}:dlq` stream (Redis). The commands work the same on all of them.

## Replay and seek

`samsa:seek` moves a group's committed offset. Stop the group's workers first, or let them pick up the change after their next lease renewal.

```bash
php artisan samsa:seek billing orders --to=earliest                  # everything still retained
php artisan samsa:seek billing orders --to=latest                    # skip the backlog
php artisan samsa:seek billing orders --to=time:2026-10-11T00:00:00Z # first record at/after this time
php artisan samsa:seek billing orders --to=offset:1200 --partition=3
php artisan samsa:seek billing orders --to=group:billing-old         # copy another group's positions
php artisan samsa:seek billing orders --to=earliest --dry-run        # show what would change
```

In code:

```php
Samsa::offsets('billing', 'orders')->seekToTimestamp(now()->subDay());
```

Typical uses:

- **Bug fix:** a handler mis-processed records. Fix it and seek back to before the bug shipped.
- **New read model:** create a new group and let it start from `earliest`.
- **Skip a poisoned backlog:** seek to `latest` and handle the gap by other means.

Replay re-runs handlers, so they must be idempotent. Replay can't go further back than [retention](operations.md#retention).

## Batch handlers

For high-throughput work (bulk inserts, search indexing), implement `BatchHandler` to receive a whole batch from one partition:

```php
use Siberfx\Samsa\Contracts\BatchHandler;

final class IndexProducts implements BatchHandler
{
    /** @param Record[] $records  all from the same partition, in order */
    public function handleBatch(array $records): void
    {
        $this->search->upsert(array_map(fn (Record $r) => $r->payload, $records));
    }
}
```

If `handleBatch()` throws, the whole batch is retried. After the last attempt, Samsa falls back to calling the batch handler with one record at a time, so that only the records that really fail are dead-lettered.

## Consumer events

| Event | When |
|---|---|
| `RecordConsumed` | A handler succeeded |
| `RecordFailed` | An attempt failed (fires on every attempt) |
| `RecordDeadLettered` | A record was moved to the dead letter store |
| `PartitionAssigned` | This worker acquired a partition |
| `PartitionRevoked` | This worker released or lost a partition |
| `WorkerStopping` | Graceful shutdown started |

All are in `Siberfx\Samsa\Events`. Every worker log entry includes `group`, `topic`, `partition` and `offset` as context.

## Running in production

Run workers under a process manager. Supervisor example:

```ini
[program:samsa-billing]
process_name=%(program_name)s_%(process_num)02d
command=php /var/www/app/artisan samsa:consume billing orders --max-time=3600 --memory=256
autostart=true
autorestart=true
stopsignal=TERM
stopwaitsecs=60
numprocs=4
user=www-data
redirect_stderr=true
stdout_logfile=/var/www/app/storage/logs/samsa-billing.log
```

- **`stopwaitsecs`** must be longer than your slowest record plus its retries, so graceful shutdown can finish.
- **Deploys:** run `php artisan samsa:restart` after deploying new code, just like `queue:restart`.
- **One program per group** keeps scaling and logs separate.
- **Laravel Cloud / Forge / Vapor:** run `samsa:consume` wherever you'd run a long-lived `queue:work` daemon. Serverless platforms that can't hold a long-running process aren't supported.

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| Lag grows on one partition only | A hot key, or a record stuck in retries | `samsa:status --partition=N`; check failing records in the logs |
| A worker owns no partitions | More workers than partitions | Expected (standby); add partitions if you need more parallelism |
| The same record is processed twice | Crash or rebalance before commit (at-least-once) | Make the handler idempotent; try `--commit=record` |
| Takeover after a crash takes 30s | `lease_seconds` | Lower it, keeping it above your slowest record |
| `LeaseLostException` in the logs | The worker paused longer than `lease_seconds` (GC, slow I/O) | Raise `lease_seconds`, or speed up the handler |
| A new group processed old history | `start_from = earliest` (default) | `samsa:seek group topic --to=latest` before starting |
