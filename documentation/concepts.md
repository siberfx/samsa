# Concepts

Samsa is a small implementation of the **partitioned log** idea popularised by Apache Kafka. If you know Kafka, skim the [mapping table](#samsa-vs-kafka-vs-laravel-queues). If you don't, this page covers everything you need.

- [The log](#the-log)
- [Topics and partitions](#topics-and-partitions)
- [Keys and ordering](#keys-and-ordering)
- [Offsets](#offsets)
- [Consumer groups](#consumer-groups)
- [Workers, leases and rebalancing](#workers-leases-and-rebalancing)
- [Commits and delivery guarantees](#commits-and-delivery-guarantees)
- [Failures, retries and dead letters](#failures-retries-and-dead-letters)
- [Retention](#retention)
- [Samsa vs Kafka vs Laravel queues](#samsa-vs-kafka-vs-laravel-queues)

## The log

A **log** is an append-only, ordered sequence of records. You only add to the end; you never update or delete in the middle. Readers keep their own bookmark into it.

```
offset:   0      1      2      3      4      5
        ┌──────┬──────┬──────┬──────┬──────┬──────┐
        │ rec  │ rec  │ rec  │ rec  │ rec  │ rec  │ ← new records appended here
        └──────┴──────┴──────┴──────┴──────┴──────┘
                      ▲                    ▲
               emails group          billing group
               (committed 1)         (committed 4)
```

Two things follow from this:

- **Reading doesn't remove anything.** Any number of independent readers can process the same records.
- **History can be replayed.** Moving a bookmark back reprocesses records. This is useful for bug fixes, new features and rebuilding read models.

## Topics and partitions

A **topic** (e.g. `orders`) is a named log. To allow parallel processing it is split into **N partitions**. Each partition is its own independent, ordered log.

```
topic: orders (4 partitions)

partition 0 │ o-17 │ o-3  │ o-17 │ o-88 │
partition 1 │ o-42 │ o-42 │ o-5  │
partition 2 │ o-9  │ o-61 │ o-9  │ o-9  │ o-2 │
partition 3 │ o-13 │
```

- Order is guaranteed **within** a partition, never **across** partitions.
- A partition is processed by **at most one worker per group** at a time. The partition count is therefore the upper bound on parallelism per group.
- The partition count is fixed when the topic is created. See [Choosing a partition count](producing.md#choosing-a-partition-count).

## Keys and ordering

Every record may have a **key**. The partitioner maps it to a partition:

```
partition = crc32(key) % partitionCount
```

So **all records with the same key go to the same partition, in the order they were produced**. Pick the key that defines "things that must happen in order":

| Topic | Good key | Why |
|---|---|---|
| `orders` | order ID | `placed` → `paid` → `shipped` for one order stays in order |
| `accounts` | account ID | balance changes for one account are applied in sequence |
| `page-views` | none (`null`) | order doesn't matter; spread load evenly |

Records with a `null` key are spread round-robin and have no ordering guarantee.

> **Hot keys:** a single very busy key (one tenant producing 90% of traffic) always lands on one partition, and therefore on one worker. Choose a key with enough distinct values.

## Offsets

An **offset** is a record's position in its partition. It is assigned when the record is written and never changes.

- Offsets only increase within a partition.
- In Samsa they are **opaque strings**. Database and NATS offsets look like `"0"`, `"1"`, `"2"`, while Redis offsets look like `"1729270000000-0"`. Don't do arithmetic on them. Use them to seek and log, and let the driver compare them.
- The Database driver guarantees offsets become **visible in order**: a consumer can never see offset 11 and then later discover offset 10. See [Database driver → Ordering](drivers/database.md#how-ordering-is-guaranteed).

## Consumer groups

A **consumer group** is a name (e.g. `billing`) that owns one bookmark per partition: the **committed offset**.

```
                      ┌──────────── orders ────────────┐
                      │ p0     p1     p2     p3        │
group billing  ─────▶ │ @12    @9     @30    @4        │
group emails   ─────▶ │ @2     @9     @11    @4        │
group search   ─────▶ │ @12    @9     @30    @4        │
                      └────────────────────────────────┘
```

- Each group receives **every** record of the topics it subscribes to.
- Within a group, each record is handled by **one** worker.
- Groups never affect each other. A slow `emails` group doesn't slow `billing`.
- When a group starts for the first time it begins at the earliest record, or at the end of the log if `consumer.start_from` is `latest`.

**Rule of thumb:** one group per *purpose* (billing, notifications, search indexing), not per worker.

## Workers, leases and rebalancing

A **worker** is one `php artisan samsa:consume` process. It joins a group as a **member** and claims partitions with **leases**. A lease is a time-limited lock that the worker keeps renewing.

```
group billing, topic orders (8 partitions)

1 worker:   worker-A ▶ p0 p1 p2 p3 p4 p5 p6 p7
2 workers:  worker-A ▶ p0 p1 p2 p3      worker-B ▶ p4 p5 p6 p7
3 workers:  worker-A ▶ p0 p1 p2         worker-B ▶ p3 p4 p5     worker-C ▶ p6 p7
9 workers:  8 workers own one partition each; worker-I is idle (standby)
```

- **Fair share:** each member aims for `ceil(partitions / members)`. When a member joins, the others release their surplus partitions.
- **Graceful shutdown** (`SIGTERM`): the worker finishes the current record, commits, and releases its leases immediately.
- **Crash** (`kill -9`, OOM, lost server): the leases simply expire after `lease_seconds` (default 30). Another member claims them and continues from the **last committed offset**.
- **Fencing:** a worker that lost its lease (for example after a long GC pause) is not allowed to commit. That stops a "zombie" worker from moving a group's offset backwards or forwards.

## Commits and delivery guarantees

A worker **commits** an offset after it has handled the records up to it. By default it commits once per batch (`consumer.commit = batch`), and you can switch to once per record (`record`).

Samsa guarantees **at-least-once** delivery:

- A record is never skipped. Its offset is only committed after the handler returns.
- A record **may be delivered more than once**, for example when a worker crashes after handling records but before committing them. The next owner re-reads from the last commit.

```
handle(10) ✓  handle(11) ✓  handle(12) ✓  💥 crash before commit (last commit = 9)
                                         → next worker re-delivers 10, 11, 12
```

So **handlers must be idempotent**: running one twice must have the same effect as running it once. See [Idempotency](idempotency.md) for patterns, and `record` commit mode if you want to shrink the redelivery window.

Samsa does **not** provide exactly-once processing.

## Failures, retries and dead letters

When a handler throws:

1. The record is **retried in place**, up to `retry.max_attempts` times (default 3), waiting `retry.backoff_ms` between attempts.
2. If it still fails, it is written to the **dead letter store** with the exception and attempt count.
3. Its offset is **committed** and the worker moves on to the next record.

Why in place? Because the log is ordered. Skipping record 10 to retry it after 11 would break per-key ordering, while waiting forever would block the partition. A bounded retry followed by a dead letter is the standard compromise.

Dead letters can be listed, inspected and re-produced to the original topic once the cause is fixed. See [Consuming → Dead letters](consuming.md#dead-letters).

Throw `UnrecoverableRecordException` (for example for invalid payloads) to skip the retries and dead-letter the record immediately.

## Retention

Logs grow forever unless you trim them. Each topic has a **retention policy**:

- `max_age_seconds`: delete records older than this (default 7 days),
- `max_records_per_partition`: keep at most N records per partition,
- `respect_slowest_consumer`: never delete records a group hasn't committed yet.

The Database driver applies retention when `samsa:prune` runs (schedule it). NATS and Redis apply it natively. See [Operations → Retention](operations.md#retention).

Replay only works as far back as retention allows.

## Samsa vs Kafka vs Laravel queues

| | Laravel queue | **Samsa** | Apache Kafka |
|---|---|---|---|
| Model | Job queue: a job is deleted after it runs | Partitioned log: records stay until retention removes them | Partitioned log |
| Readers per message | One | One per **group**, any number of groups | One per group |
| Ordering | None guaranteed | Per key | Per key |
| Replay | No | Yes (`samsa:seek`) | Yes |
| Parallelism | Number of workers | Number of partitions (per group) | Number of partitions |
| Infrastructure | DB / Redis / SQS… | DB, NATS or Redis | A Kafka cluster |
| Replication / multi-broker | Depends on backend | Only via NATS (R3/R5) | Yes |
| Delivery | At-least-once | At-least-once | At-least-once / exactly-once (transactions) |
| PHP requirement | — | Pure PHP | `ext-rdkafka` (for PHP clients) |

### Concept mapping

| Kafka | Samsa (all drivers) | Database driver | NATS JetStream | Redis Streams |
|---|---|---|---|---|
| Topic | Topic | `samsa_topics` row | Stream `SAMSA_{topic}` | Key prefix |
| Partition | Partition | `samsa_log.partition` | Subject `samsa.{topic}.{N}` | Stream `{prefix}{topic}:p{N}` |
| Offset | Offset (string) | Per-partition counter | Stream sequence | Entry ID |
| Consumer group | Group | `samsa_offsets.group_name` | State entry `offsets.{group}.{topic}.{N}` | `XGROUP` |
| Commit | Commit | Row upsert | State entry update | `XACK` |
| Group coordinator | Leases | `samsa_leases` | Compare-and-swap state entries | `SET NX PX` + Lua |
| DLQ (by convention) | Dead letter store | `samsa_failed` | State entries `dlq.*` | `{topic}:dlq` stream |
