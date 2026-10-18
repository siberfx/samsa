# Samsa Documentation

Samsa brings Kafka's event log model into Laravel: **topics** split into ordered **partitions**, records addressed by **offset**, and **consumer groups** that each read the full log at their own pace. It runs on your database, NATS JetStream or Redis, in pure PHP.

> Everything here is in 1.0.0 unless a page or section is marked **Planned** or with a later version such as **`v1.1`**.

## Learn

1. **[Getting started](getting-started.md)** — install, create a topic, produce, consume. About 10 minutes.
2. **[Concepts](concepts.md)** — topics, partitions, offsets, groups, leases, delivery guarantees. Read this before going to production.
3. **[Configuration](configuration.md)** — every key in `config/samsa.php`.

## Guides

| Guide | Covers |
|---|---|
| [Producing records](producing.md) | Keys, headers, batches, dedup, producing inside transactions, choosing a partition count |
| [Consuming records](consuming.md) | Handlers, groups, workers, commits, retry, dead letters, replay, Supervisor |
| [Queue driver](queue-driver.md) | Running Laravel jobs on Samsa, `later()`, `release()`, failures, limits |
| [Event bridge](event-bridge.md) | `#[PublishTo]`, `Samsa::publish()`, forwarding broadcast events |
| [message-bus transport](message-bus-integration.md) | Using Samsa as a `siberfx/message-bus` transport |
| [Idempotency](idempotency.md) | Living with at-least-once delivery |

## Drivers

| Driver | Pick it when | Docs |
|---|---|---|
| Database (default) | You want no new infrastructure, or transactional produce | [database.md](drivers/database.md) |
| NATS JetStream | You want high throughput and the closest thing to Kafka | [nats.md](drivers/nats.md) |
| Redis Streams | Redis is already in your stack | [redis-streams.md](drivers/redis-streams.md) |
| Memory | Tests | [testing.md](testing.md) |

## Reference

- [Operations & Artisan commands](operations.md) — status, lag, seek, prune, failed records, deployment, monitoring
- [Testing](testing.md) — `Samsa::fake()`, the memory driver, integration tests
- [FAQ](faq.md) — including *when not to use Samsa*

## Glossary

| Term | Meaning |
|---|---|
| **Topic** | A named log, e.g. `orders`. |
| **Partition** | One of a topic's N ordered sub-logs. A key always maps to the same partition. |
| **Record** | One entry in a partition: key, payload, headers, offset, timestamp. |
| **Offset** | A record's position in its partition. Opaque string; only increases. |
| **Consumer group** | A named set of workers sharing one position in the log, e.g. `billing`. |
| **Committed offset** | The last offset a group has finished, per partition. |
| **Lag** | How many records a group is behind the end of a partition. |
| **Lease** | A worker's temporary, renewable claim on a partition for its group. |
| **Member** | One running worker process in a group. |
| **Dead letter** | A record that failed all retries, stored for inspection and retry. |
