# Operations & Artisan Commands

- [Command reference](#command-reference)
- [Topics](#topics)
- [Status and lag](#status-and-lag)
- [Retention](#retention)
- [Scheduler entries](#scheduler-entries)
- [Deploying](#deploying)
- [Monitoring](#monitoring)
- [Switching drivers](#switching-drivers)
- [Backups and disaster recovery](#backups-and-disaster-recovery)
- [Capacity planning](#capacity-planning)

## Command reference

Every command accepts `--driver=` to target a non-default driver.

| Command | Purpose |
|---|---|
| **Topics** | |
| `samsa:topic:create {name?} --partitions= [--from-config]` | Create a topic, or every topic declared in config |
| `samsa:topic:list` | Topics with partition counts, record counts and retention |
| `samsa:topic:alter {name} --partitions= --force` | Increase partitions (remaps keys; see [Producing](producing.md#choosing-a-partition-count)) |
| `samsa:topic:delete {name} --force` | Delete a topic, its records and all group offsets for it |
| **Producing** | |
| `samsa:produce {topic} {json} [--key=] [--header=k:v]* [--type=]` | Produce one record (for testing and ops) |
| `samsa:tail {topic} [--partition=] [--from=latest]` | Print records as they arrive (like `tail -f`) |
| **Consuming** | |
| `samsa:consume {group} {topic*} [options]` | Run a worker. See [Worker options](consuming.md#worker-options) |
| `samsa:restart` | Ask all workers to exit gracefully after their current batch |
| `samsa:seek {group} {topic} --to= [--partition=] [--dry-run]` | Move a group's offsets. See [Replay and seek](consuming.md#replay-and-seek) |
| `samsa:groups` | All groups with their topics, members and total lag |
| **Inspection** | |
| `samsa:status [--group=] [--topic=] [--partition=] [--json]` | Per-partition latest offset, committed offset, lag, owner, lease expiry |
| **Dead letters** | |
| `samsa:failed [--group=] [--topic=]` | List dead letters |
| `samsa:failed:show {id}` | Payload, headers, exception and stack trace |
| `samsa:failed:retry {id?} [--all] [--group=] [--in-place]` | Re-produce (or re-handle in place) |
| `samsa:failed:forget {id}` | Delete one dead letter |
| `samsa:failed:flush [--older-than=]` | Delete dead letters in bulk |
| **Maintenance** | |
| `samsa:prune [--topic=] [--dry-run] [--processed-older-than=]` | Apply retention; clean the `OnlyOnce` table |
| `samsa:release-delayed` (`v1.1`) | Move due delayed records into their topics |

## Topics

Declare your topology in `config/samsa.php` and apply it on deploy. That way every environment has the same topics:

```php
'topics' => [
    'orders'       => ['partitions' => 16],
    'payments'     => ['partitions' => 16, 'retention' => ['max_age_seconds' => 60 * 60 * 24 * 90]],
    'page-views'   => ['partitions' => 4,  'retention' => ['max_age_seconds' => 60 * 60 * 24]],
    'laravel-jobs' => ['partitions' => 8],
],
```

```bash
php artisan samsa:topic:create --from-config   # idempotent; safe to run on every deploy
```

`--from-config` creates missing topics and updates retention. It never shrinks partitions, and it refuses to grow them without `--force`.

## Status and lag

```bash
php artisan samsa:status --group=billing
```

```
 Group    Topic   Part  Latest  Committed  Lag   Owner          Lease until
 billing  orders  0     10423   10423      0     app-2:31877    12:04:51
 billing  orders  1     9877    9870       7     app-2:31877    12:04:51
 billing  orders  2     11002   10204      798   app-1:2201     12:04:49   ⚠
 billing  orders  3     10340   10340      0     —              —          ⚠ unowned
```

- **Lag** is the number of records the group hasn't committed yet. Lag that keeps growing on one partition means a hot key or a slow or stuck record. Lag growing on every partition means not enough workers, or slow handlers.
- **Unowned** partitions with lag mean there are fewer live workers than expected.
- `--json` prints machine-readable output for scripts and alerts.

In code, for health checks and dashboards:

```php
$lag = Samsa::stats()->lag('billing', 'orders');           // int, total
$parts = Samsa::stats()->partitions('billing', 'orders');  // per-partition details
```

## Retention

Records are deleted according to each topic's retention policy:

| Driver | How retention runs |
|---|---|
| Database | `samsa:prune` deletes in chunks of 5,000 rows per partition. **Schedule it.** |
| NATS | Natively by JetStream (`max_age`, `max_msgs_per_subject`), so `samsa:prune` is a no-op |
| Redis | `XTRIM` on produce (approximate `MAXLEN`) plus `samsa:prune` for age-based `MINID` trimming |

With `respect_slowest_consumer: true`, nothing that any known group hasn't committed yet is deleted, which avoids data loss when a group is far behind. The trade-off is that an abandoned group stops retention for good. Delete abandoned groups with `samsa:seek {group} {topic} --to=latest` or remove their offsets.

## Scheduler entries

Samsa registers these automatically. You only need the scheduler running:

| Task | Frequency | Can be disabled with |
|---|---|---|
| `samsa:prune` | hourly | `SAMSA_SCHEDULE_PRUNE=false` |
| `samsa:release-delayed` (`v1.1`, with the queue driver) | every second | `SAMSA_SCHEDULE_DELAYED=false` |

Production cron:

```cron
* * * * * cd /var/www/app && php artisan schedule:run >> /dev/null 2>&1
```

Sub-minute tasks need `schedule:run` to be invoked every minute, as usual. Laravel then keeps the process alive for the remainder of the minute.

## Deploying

1. Deploy code and run migrations (`samsa_*` tables).
2. `php artisan samsa:topic:create --from-config`.
3. `php artisan samsa:restart`, plus `queue:restart` if you use the queue driver.
4. Supervisor restarts the workers with the new code. They resume from their committed offsets.

**Rolling deploys** across several servers are safe. Old and new workers coexist in the group, and partitions move between them through the normal lease mechanism. Make sure new handler code can still process records produced by old code (and vice versa) during the overlap. See [Payload design](producing.md#payload-design).

**Zero-downtime handler changes:** if a new handler version must reprocess history, deploy it under a **new group name** that starts from `earliest`, let it catch up, then switch over and retire the old group.

## Monitoring

Watch these signals:

| Signal | Source | Alert when |
|---|---|---|
| Lag per group | `samsa:status --json`, `Samsa::stats()->lag()` | Growing for > N minutes |
| Dead letters | `RecordDeadLettered` event, `samsa:failed` | Any new entry (or a rate above a threshold) |
| Unowned partitions | `samsa:status --json` | Unowned and lag > 0 for > `lease_seconds × 2` |
| Worker restarts | Supervisor logs, `WorkerStopping` event | Frequent restarts (crash loop) |
| Produce errors | `QueryException` / `DriverException` in your error tracker | Any |

Example: a health endpoint for your uptime monitor:

```php
Route::get('/health/samsa', function () {
    $lag = Samsa::stats()->lag('billing', 'orders');

    return response()->json(['lag' => $lag], $lag > 10_000 ? 503 : 200);
});
```

Laravel Pulse: a Samsa recorder and card (lag per group, throughput, dead letters) is planned. Until then, record `RecordConsumed` / `RecordDeadLettered` events with a custom Pulse recorder.

## Switching drivers

Drivers don't share storage, so switching moves *new* records. History is not migrated automatically:

1. Create the topics on the new driver (`samsa:topic:create --from-config --driver=nats`).
2. Start consumers on the new driver (`--driver=nats`) alongside the old ones.
3. Switch producers (`SAMSA_DRIVER=nats`).
4. Let the old consumers drain (lag 0), then stop them.

If you need history on the new driver, run a relay group that reads the old topic from `earliest` and produces to the new driver with `dedupKey($record->messageId())`. See the [relay pattern](producing.md#guaranteed-delivery-to-nats-or-redis-the-relay-pattern).

## Backups and disaster recovery

- **Database driver:** the log and offsets live in your database, so your normal database backups cover them. Restoring a backup restores records and offsets **consistently**, from the same point in time.
- **NATS:** use JetStream stream snapshots (`nats stream backup`) or R3 replication. Lease and offset state lives in JetStream too.
- **Redis:** use RDB/AOF persistence. Without persistence, Redis Streams is not durable.

After a restore, consumers resume from the restored offsets. Anything produced after the backup is gone, and effects of records consumed after the backup may be repeated, so idempotent handlers matter here too.

## Capacity planning

Rough Database-driver numbers to start from. Measure on your own hardware (`benchmarks/` in the repo):

| Factor | Effect |
|---|---|
| Produce | One `SELECT … FOR UPDATE` + one insert per record. Use `produceBatch()` for bulk writes |
| Partitions | Writes to different partitions run in parallel; writes to one partition are serialized |
| Consume | One indexed range read per partition per batch + one upsert per commit |
| Table size | Plan for `records/day × retention days × average record size`, plus about 30% for indexes |

If you need more than a few thousand records per second sustained, use the [NATS driver](drivers/nats.md).
