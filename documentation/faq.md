# FAQ

- [Is Samsa a Kafka client?](#is-samsa-a-kafka-client)
- [Why "Samsa"?](#why-samsa)
- [When should I not use Samsa?](#when-should-i-not-use-samsa)
- [Samsa or Laravel queues?](#samsa-or-laravel-queues)
- [Which driver should I pick?](#which-driver-should-i-pick)
- [Do I get exactly-once processing?](#do-i-get-exactly-once-processing)
- [Is ordering global across a topic?](#is-ordering-global-across-a-topic)
- [How many records per second can it handle?](#how-many-records-per-second-can-it-handle)
- [Can non-PHP services read the log?](#can-non-php-services-read-the-log)
- [Can I use it with Horizon?](#can-i-use-it-with-horizon)
- [Does it work on serverless (Vapor)?](#does-it-work-on-serverless-vapor)
- [Can I change the number of partitions later?](#can-i-change-the-number-of-partitions-later)
- [What happens if a handler is slow?](#what-happens-if-a-handler-is-slow)
- [Can I delete a single record (GDPR)?](#can-i-delete-a-single-record-gdpr)
- [Is it multi-tenant friendly?](#is-it-multi-tenant-friendly)

## Is Samsa a Kafka client?

No. Samsa doesn't speak Kafka's protocol and can't talk to a Kafka cluster. It implements Kafka's **model** (topics, partitions, offsets, consumer groups) on top of a database, NATS JetStream or Redis. If you already run Kafka, use a Kafka client such as `mateusjunges/laravel-kafka`.

## Why "Samsa"?

Gregor Samsa is the character in Franz Kafka's *The Metamorphosis* who wakes up transformed. Samsa is Kafka's model, transformed to fit inside a Laravel app.

## When should I not use Samsa?

- You need **multi-datacenter replication** or a cluster that survives losing machines at very high throughput. Run Kafka (or Redpanda, or a NATS cluster with R3 and the NATS driver).
- You need **exactly-once** semantics across systems.
- You need **hundreds of thousands of records per second** from one app.
- You only need **fire-and-forget background jobs** with no ordering or replay. Laravel queues are simpler.
- Your workers must run on a **platform without long-running processes**.

## Samsa or Laravel queues?

| Need | Use |
|---|---|
| "Do this work later, once" | Laravel queues |
| "Several parts of the system react to this event independently" | Samsa topics |
| "Changes for the same entity must be applied in order" | Samsa (by key), or the Samsa queue driver |
| "I need to reprocess last week's events after a bug fix" | Samsa |
| "I want an audit log I can rebuild projections from" | Samsa |

They work well together. A Samsa handler can dispatch Laravel jobs, and the [queue driver](queue-driver.md) runs jobs on Samsa.

## Which driver should I pick?

Start with **Database**. It needs nothing new, gives you transactional produce, and your backups already cover it. Move to **NATS JetStream** when throughput, latency or replication matter. Pick **Redis Streams** if Redis is already your durable infrastructure, with persistence configured. Handlers don't change when you switch. See [Switching drivers](operations.md#switching-drivers).

## Do I get exactly-once processing?

No. Delivery is **at-least-once**. A record is never lost once produced, but it can be handled more than once after crashes, rebalances or replays. Make handlers idempotent: see [Idempotency](idempotency.md). With the Database driver, if your handler's side effects live in the same database, `OnlyOnce` gives you effectively-once behaviour for those effects.

## Is ordering global across a topic?

No, ordering is **per key** (strictly, per partition). Records with the same key are always handled in order. Records with different keys may be handled in any order relative to each other. If you need total order across a whole topic, create it with one partition and accept one worker per group.

## How many records per second can it handle?

It depends on the driver, the hardware and the handlers. As an order of magnitude: the Database driver handles **hundreds to a few thousand** records per second per topic, NATS JetStream **tens of thousands or more**, and Redis Streams **tens of thousands**. Handler speed is usually the real limit. Add partitions and workers to scale consuming. The repo's `benchmarks/` directory lets you measure your own setup.

## Can non-PHP services read the log?

Yes. Records are JSON with plain headers.

- **NATS:** any NATS client can consume the `SAMSA_{TOPIC}` streams. Subjects are `samsa.{topic}.p{N}`.
- **Redis:** any client can `XREAD` the `{prefix}{topic}:p{N}` streams.
- **Database:** read `samsa_log` ordered by `(partition, offset)`.

Non-PHP consumers should manage their own position rather than write to Samsa's offset and lease tables.

## Can I use it with Horizon?

No. Horizon only supports Laravel's Redis queue driver. Use `samsa:status`, `Samsa::stats()` and your monitoring stack. See [Monitoring](operations.md#monitoring).

## Does it work on serverless (Vapor)?

Producing works anywhere. Consuming needs a long-running `samsa:consume` process, which serverless functions can't host. Run workers on a server, a container or a platform with daemons, such as Forge, Laravel Cloud workers or Kubernetes.

## Can I change the number of partitions later?

You can **increase** it (`samsa:topic:alter --partitions=N --force`), but keys get remapped, so per-key ordering isn't guaranteed while old records drain. You can't decrease it. See [Choosing a partition count](producing.md#choosing-a-partition-count).

## What happens if a handler is slow?

The partition it's working on waits. Other partitions, and other groups, keep going. A handler must finish within `lease_seconds` (Samsa renews the lease between records and during retry backoff, not during a single long call). Move genuinely long work out of the handler: dispatch a Laravel job from it and return.

## Can I delete a single record (GDPR)?

A log is append-only by design. For personal data:

- Don't put personal data in payloads. Reference a user ID and keep the data in a table you can erase.
- Or encrypt personal fields with a per-user key (a custom [serializer](configuration.md#serializer-and-partitioner)) and delete the key on erasure ("crypto-shredding").
- Retention eventually removes old records.

The Database driver technically allows deleting rows, but consumers that already processed a record won't "un-process" it.

## Is it multi-tenant friendly?

Yes. Common setups:

- **Shared topics, tenant in the key or header.** Key by `"{tenant}:{entity}"` to keep each entity ordered, and put `tenant` in the headers for filtering.
- **Topic per tenant** (`orders.acme`, `orders.globex`) when tenants need separate retention or replay.
- **Database per tenant:** point `SAMSA_DB_CONNECTION` at the tenant connection, or call `Samsa::driver()` with a per-tenant configuration.

Avoid letting a single huge tenant become a [hot key](concepts.md#keys-and-ordering).
