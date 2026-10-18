# Queue Driver

> **Planned for v1.1.** This page documents the design of a feature that is not in 1.0.0 yet. Follow the [changelog](../CHANGELOG.md) for its release.

Samsa ships a Laravel queue driver, so existing jobs run on the event log without code changes:

```dotenv
QUEUE_CONNECTION=samsa
```

```php
SendWelcomeEmail::dispatch($user);          // unchanged
```

```bash
php artisan queue:work samsa                # unchanged
```

- [When to use it](#when-to-use-it)
- [Setup](#setup)
- [How jobs map to the log](#how-jobs-map-to-the-log)
- [Ordering jobs per key](#ordering-jobs-per-key)
- [Multiple queues](#multiple-queues)
- [Delayed jobs](#delayed-jobs)
- [Releasing, retries and failures](#releasing-retries-and-failures)
- [Queue size](#queue-size)
- [Workers](#workers)
- [Limitations](#limitations)

## When to use it

| Use the queue driver when… | Use topics and handlers when… |
|---|---|
| You have existing jobs and want **ordered, replayable** execution | You're publishing **events** that several parts of the system react to |
| Exactly one worker should run each job | Several independent groups should see each record |
| You want `$tries`, `$backoff`, `failed()` to behave as usual | You want explicit control over groups, commits and replay |

Both can run in the same app.

## Setup

Add a connection to `config/queue.php`:

```php
'connections' => [
    'samsa' => [
        'driver'       => 'samsa',
        'topic'        => 'laravel-jobs',  // default topic for jobs on this connection
        'group'        => 'laravel-queue', // the consumer group queue:work joins
        'after_commit' => false,
    ],
],
```

Create the topic (or declare it in `config/samsa.php` under `topics`):

```bash
php artisan samsa:topic:create laravel-jobs --partitions=8
```

## How jobs map to the log

| Laravel | Samsa |
|---|---|
| `dispatch($job)` | `produce()` of the serialized job payload to the connection's topic |
| Queue name (`onQueue('emails')`) | Topic. `default` maps to the connection's `topic`, any other name maps to a topic of that name |
| `queue:work` process | A member of the connection's `group` |
| Job finished (`delete()`) | Offset committed |
| `release($delay)` | Job re-produced (delayed) + offset committed |
| Job failed | Laravel's `failed()` + `failed_jobs` as usual, then offset committed |

The job payload is Laravel's standard queue payload, so job middleware, `ShouldBeUnique`, batching, chaining, encryption (`ShouldBeEncrypted`) and `Queue::route()` all keep working.

## Ordering jobs per key

By default jobs have no key and are spread round-robin. Add a `samsaKey()` method to run jobs **for the same entity strictly in order, one at a time**:

```php
final class SyncInventory implements ShouldQueue
{
    use Queueable;

    public function __construct(public int $productId, public int $delta) {}

    public function samsaKey(): string
    {
        return "product-{$this->productId}";
    }

    public function handle(): void { /* ... */ }
}
```

All `SyncInventory` jobs for product 7 now run in dispatch order and never concurrently, and you don't need `WithoutOverlapping` locks.

## Multiple queues

```php
ProcessPodcast::dispatch($podcast)->onQueue('media');   // topic "media"
```

```bash
php artisan queue:work samsa --queue=media,laravel-jobs
```

Each queue name is a topic, and the worker claims partitions of each listed topic. Note that `--queue` priority order is respected within each loop, but a busy higher-priority topic doesn't fully starve the others the way it does on the Redis driver.

## Delayed jobs

```php
SendReminder::dispatch($user)->delay(now()->addHours(2));
```

A log can't hold a record back, so delayed jobs wait in a **delayed store** (`config('samsa.delayed.store')`, the `samsa_delayed` table or a Redis sorted set). They are produced to their topic when due by:

```bash
php artisan samsa:release-delayed
```

Samsa registers this in the scheduler to run **every second**, so make sure the scheduler is running (`php artisan schedule:work` locally, cron in production).

Delayed jobs are **ordered by due time, not dispatch time**. A delayed job for key K runs after any non-delayed jobs for K that were dispatched while it waited.

## Releasing, retries and failures

Everything behaves the way Laravel users expect:

```php
final class CallFlakyApi implements ShouldQueue
{
    public int $tries = 5;
    public array $backoff = [10, 60, 300];

    public function handle(): void
    {
        if (RateLimiter::tooManyAttempts('api', 100)) {
            $this->release(30);   // re-produced via the delayed store, offset committed
            return;
        }
        // ...
    }

    public function failed(Throwable $e): void { /* ... */ }
}
```

- An exception → `$tries` / `$backoff` → `release()` via the delayed store.
- Final failure → `failed()` + the `failed_jobs` table, and `queue:retry` works as usual.
- Samsa's own `retry.*` settings and dead-letter store are **not** used for queue jobs, because Laravel's job retry logic applies instead.

> **Ordering note:** a released job goes to the **end** of its partition. If a keyed job must not be overtaken by later jobs with the same key, throw instead of releasing and set `$backoff`. The worker then retries it in place, which blocks the key until it succeeds or fails.

## Queue size

```php
Queue::connection('samsa')->size('laravel-jobs');
```

This returns the total **lag** of the connection's group on that topic: records produced but not yet committed. Delayed jobs that haven't been released yet aren't counted. `php artisan samsa:status --group=laravel-queue` shows the per-partition breakdown.

## Workers

```bash
php artisan queue:work samsa --queue=laravel-jobs --tries=3 --max-time=3600
```

Each `queue:work` process joins the connection's group and gets a fair share of partitions, exactly like `samsa:consume`. All the usual flags work (`--once`, `--stop-when-empty`, `--max-jobs`, `--memory`, `--timeout`), as do `queue:restart` and Supervisor configs.

Parallelism is capped at the partition count. Eight partitions with twenty workers means twelve idle workers.

## Limitations

- **Horizon is not supported.** Horizon only works with the Redis queue driver. Use `samsa:status` and your own monitoring instead.
- `queue:clear` seeks the group to `latest`. It doesn't delete records, which stay in the log until retention removes them.
- `queue:monitor` works through `size()`.
- `--timeout` kills a stuck job as usual. The worker then exits, its leases expire, and another worker re-runs the job: at-least-once, as with every Laravel driver.
- Delayed-job precision is about 1 second (the scheduler interval).
