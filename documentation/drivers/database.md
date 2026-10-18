# Database Driver

The default driver. It stores the log, offsets, leases and dead letters in your existing database (MySQL 8+, MariaDB 10.6+, PostgreSQL 13+ or SQLite 3.35+).

- [Why use it](#why-use-it)
- [Setup](#setup)
- [Tables](#tables)
- [How ordering is guaranteed](#how-ordering-is-guaranteed)
- [How leases work](#how-leases-work)
- [Transactional produce](#transactional-produce)
- [Performance and tuning](#performance-and-tuning)
- [Database-specific notes](#database-specific-notes)
- [Querying the log directly](#querying-the-log-directly)

## Why use it

- **No new infrastructure.** It uses the database you already run, monitor and back up.
- **Transactional produce.** Records commit or roll back together with your business data, so you don't need an outbox.
- **Consistent backups.** Records and offsets are restored from the same point in time.
- **Inspectable.** Everything is a table you can query.

The trade-off is throughput: writes to one partition are serialized, and a busy log adds load to your primary database. See [Performance and tuning](#performance-and-tuning).

## Setup

```dotenv
SAMSA_DRIVER=database
SAMSA_DB_CONNECTION=        # empty = app default connection
```

```bash
php artisan vendor:publish --tag=samsa-migrations
php artisan migrate
```

To keep the log out of your main database, define a connection in `config/database.php` and set `SAMSA_DB_CONNECTION=events`. You lose transactional produce across the two connections, but keep everything else.

`drivers.database.table_prefix` is prepended to every table name, e.g. `tenant1_samsa_log`.

## Tables

| Table | Purpose |
|---|---|
| `samsa_topics` | Topic registry: name, partition count, retention |
| `samsa_partitions` | One row per partition: the **next offset counter** |
| `samsa_log` | The records. Unique on `(topic, partition, offset)` |
| `samsa_offsets` | Committed offset per `(group, topic, partition)` |
| `samsa_leases` | Partition ownership per `(group, topic, partition)` |
| `samsa_members` | Worker heartbeats, used for fair-share rebalancing |
| `samsa_failed` | Dead letters |
| `samsa_delayed` | Delayed records waiting for `samsa:release-delayed` |
| `samsa_processed` | `OnlyOnce` idempotency markers (see [Idempotency](../idempotency.md)) |

`samsa_log` columns:

| Column | Type | Notes |
|---|---|---|
| `id` | bigint | Internal surrogate key; **not** the offset |
| `topic` | string | |
| `partition` | smallint | |
| `offset` | bigint | Per-partition, contiguous, starts at 0 |
| `key` | string, nullable | |
| `dedup_key` | string, nullable | Unique per topic |
| `event_type` | string, nullable | |
| `payload` | json | |
| `headers` | json, nullable | |
| `created_at` | timestamp(6) | |

## How ordering is guaranteed

A naive log uses the auto-increment `id` as the offset. That breaks under concurrency:

```
T1: INSERT → id 10 ……(slow)…………………………… COMMIT
T2:            INSERT → id 11 → COMMIT
Consumer:                         reads 11, commits offset 11
T1 commits 10 → the consumer never reads it. Record lost.
```

Samsa gives every partition its own counter row in `samsa_partitions`. Producing does this:

```sql
BEGIN;
SELECT next_offset FROM samsa_partitions WHERE topic = ? AND partition = ? FOR UPDATE;
INSERT INTO samsa_log (..., partition, offset, ...) VALUES (..., ?, :next_offset, ...);
UPDATE samsa_partitions SET next_offset = next_offset + 1 WHERE topic = ? AND partition = ?;
COMMIT;
```

The row lock is held until commit, so the next producer for that partition waits. Offsets therefore become visible **in commit order and without gaps**. Different partitions use different rows, so producers to different partitions don't block each other.

When you produce inside your own `DB::transaction()`, those statements join your transaction and the lock is held until **your** commit. Keep such transactions short.

## How leases work

Claiming or renewing a partition is one atomic statement:

```sql
UPDATE samsa_leases
   SET member_id = :me, locked_until = :now + ttl
 WHERE group_name = :group AND topic = :topic AND partition = :p
   AND (member_id = :me OR locked_until IS NULL OR locked_until < :now);
```

`:now` is the **database server's** clock, not the worker's. Each worker measures its offset from the database clock once a minute, so clock drift between worker machines doesn't matter. One affected row means the lease is ours.

Before every commit, the worker locks its lease row (`SELECT … FOR UPDATE`) and checks `member_id = :me AND locked_until > :now` in the same transaction as the offset upsert. A worker that lost its lease therefore can't commit (fencing).

## Transactional produce

```php
DB::transaction(function () use ($order) {
    $order->update(['status' => 'paid']);
    Samsa::produce('orders', ['order_id' => $order->id, 'status' => 'paid'], key: $order->id);
});
```

This works when the Samsa connection is the same as the connection of the transaction. If `SAMSA_DB_CONNECTION` points elsewhere, Samsa throws `CrossConnectionTransactionException` when it detects an open transaction on the default connection, unless you produce with `Samsa::afterCommit()`.

## Performance and tuning

| Lever | Effect |
|---|---|
| More partitions | More parallel producers *and* consumers |
| `produceBatch()` | One lock and one multi-row insert per partition |
| A larger `--batch` | Fewer reads and commits per record |
| `--commit=batch` (default) | One offset upsert per batch instead of per record |
| A separate connection | Takes log load off your primary database (at the cost of transactional produce) |
| Retention + scheduled `samsa:prune` | Keeps `samsa_log` and its indexes small |

Idle cost: each worker issues one indexed range query per owned partition every `poll_interval_ms` when there's nothing to do. With 8 partitions and 250 ms that's 32 cheap queries per second per group, which you can tune with `--sleep`.

## Database-specific notes

| Database | Notes |
|---|---|
| **MySQL 8 / MariaDB** | Use InnoDB. `READ COMMITTED` or `REPEATABLE READ` both work. JSON columns are native |
| **PostgreSQL** | Fully supported; `jsonb` is used for payloads and headers |
| **SQLite** | Good for local development and tests. `FOR UPDATE` is ignored, but SQLite serializes writers, so ordering still holds. Use WAL mode if several processes write |
| **SQL Server** | Not supported in `v1` |

## Querying the log directly

Eloquent read models are included for debugging and admin screens:

```php
use Siberfx\Samsa\Models\LogEntry;

LogEntry::query()
    ->where('topic', 'orders')
    ->where('key', 'order-42')
    ->orderBy('partition')->orderBy('offset')
    ->get();
```

Treat these as **read-only**. Writing to the `samsa_*` tables directly bypasses offset allocation and fencing.
