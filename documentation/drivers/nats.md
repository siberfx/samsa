# NATS JetStream Driver

> **Planned for v1.2.** This page documents the design of a feature that is not in 1.0.0 yet. Follow the [changelog](../../CHANGELOG.md) for its release.

[NATS JetStream](https://docs.nats.io/nats-concepts/jetstream) is a persistent streaming layer built into the NATS server: a single ~20 MB binary with optional clustering and replication. Of Samsa's drivers it's the closest to Kafka, and it's the one to choose for high throughput.

- [Why use it](#why-use-it)
- [Run a NATS server](#run-a-nats-server)
- [Install and configure](#install-and-configure)
- [How Samsa maps to JetStream](#how-samsa-maps-to-jetstream)
- [Delivery, acks and redelivery](#delivery-acks-and-redelivery)
- [Dead letters](#dead-letters)
- [Retention and storage](#retention-and-storage)
- [Clustering and replication](#clustering-and-replication)
- [Monitoring](#monitoring)
- [Security](#security)
- [Troubleshooting](#troubleshooting)

## Why use it

- **Throughput and latency** well beyond a SQL table.
- **Replication** (R3/R5) in a cluster, so it survives losing a node.
- **Native retention** and **native dedup** (`Nats-Msg-Id`).
- It keeps the load off your application database.
- Pure PHP: the client is [`basis-company/nats`](https://github.com/basis-company/nats.php), with no extension needed.

The trade-off is one more service to run, and no transactional produce with your database (see the [relay pattern](../producing.md#guaranteed-delivery-to-nats-or-redis-the-relay-pattern)).

## Run a NATS server

Docker, for a single node:

```bash
docker run -d --name nats -p 4222:4222 -p 8222:8222 \
  -v nats-data:/data \
  nats:2.10-alpine --jetstream --store_dir=/data -m 8222
```

Docker Compose:

```yaml
services:
  nats:
    image: nats:2.10-alpine
    command: ["--jetstream", "--store_dir=/data", "-m", "8222"]
    ports: ["4222:4222", "8222:8222"]
    volumes: ["nats-data:/data"]
volumes:
  nats-data:
```

Check it's up: `curl http://localhost:8222/healthz` → `{"status":"ok"}`.

## Install and configure

```bash
composer require basis-company/nats
```

```dotenv
SAMSA_DRIVER=nats
NATS_HOST=127.0.0.1
NATS_PORT=4222
# NATS_USER= / NATS_PASS=  or  NATS_TOKEN=
```

```php
// config/samsa.php
'nats' => [
    'host'      => env('NATS_HOST', '127.0.0.1'),
    'port'      => (int) env('NATS_PORT', 4222),
    'user'      => env('NATS_USER'),
    'pass'      => env('NATS_PASS'),
    'token'     => env('NATS_TOKEN'),
    'timeout'   => 1.0,
    'reconnect' => true,
    'storage'   => 'file',   // file | memory
    'replicas'  => 1,        // 3 or 5 in a cluster
    'kv_bucket' => 'samsa_kv_leases',
],
```

Then create topics as usual:

```bash
php artisan samsa:topic:create orders --partitions=16 --driver=nats
```

## How Samsa maps to JetStream

| Samsa | JetStream |
|---|---|
| Topic `orders` | Stream `SAMSA_ORDERS`, subjects `samsa.orders.p*` |
| Partition 3 | Subject `samsa.orders.p3` |
| Offset | Stream sequence number |
| Group `billing`, partition 3 | Durable pull consumer `billing_p3`, `filter_subject: samsa.orders.p3`, `ack_policy: all` |
| Commit offset N | Ack of message N (`AckAll` acknowledges everything up to N) |
| Lease | Key `billing.orders.3` in the KV bucket `samsa_kv_leases`, updated with revision checks |
| Dedup key | `Nats-Msg-Id` header |
| Headers | NATS message headers (prefixed `Samsa-`) |

Stream sequence numbers are global to the stream, so offsets within a partition are increasing but **not contiguous** (`3, 7, 8, 12…`). That's expected. Never compute lag by subtracting offsets yourself; use `samsa:status`.

## Delivery, acks and redelivery

- Each partition consumer is configured with `max_ack_pending` = batch size and `ack_wait` > `lease_seconds`.
- Samsa still uses **its own leases** to make sure exactly one worker per group pulls a given partition. JetStream would happily deliver to several pullers in parallel, which would break per-key ordering.
- Retries happen **in place** in the worker (the same as other drivers). Samsa doesn't rely on JetStream's `max_deliver`, which is set high as a safety net only.
- If a worker crashes, unacked messages are redelivered to the next lease owner after `ack_wait`.

## Dead letters

Dead letters are published to a separate stream, `SAMSA_ORDERS_DLQ`, with the original subject, sequence, group and exception in headers, and the original is then acked. `samsa:failed*` commands work as with the other drivers.

## Retention and storage

Topic retention maps to stream limits:

| Samsa retention | JetStream |
|---|---|
| `max_age_seconds` | `max_age` |
| `max_records_per_partition` | `max_msgs_per_subject` |
| `respect_slowest_consumer` | Not supported: JetStream's `interest` retention would delete records once *all* consumers ack, which isn't the same thing. Samsa warns and ignores this flag |

`storage: memory` is faster but loses data on restart. Use it only for caches and tests.

The JetStream dedup window defaults to 2 minutes. Set `'duplicate_window' => 3600` in a topic's config for a longer one.

## Clustering and replication

Run three NATS servers with JetStream and set `'replicas' => 3`. Each stream and the lease KV bucket are then replicated, and the cluster survives one node failing. Point Samsa at one or more seeds (`NATS_HOST=nats-1,nats-2,nats-3`). The client reconnects to a healthy node automatically.

## Monitoring

| Endpoint | Shows |
|---|---|
| `http://nats:8222/healthz` | Liveness |
| `http://nats:8222/varz` | Server stats (connections, memory, throughput) |
| `http://nats:8222/jsz?streams=true&consumers=true` | Per-stream and per-consumer counters, including pending and ack-pending |

`samsa:status --driver=nats` reads consumer info and shows the same lag view as other drivers. The NATS CLI (`nats stream report`, `nats consumer report`) is useful too.

## Security

- Use credentials (user/pass, token, or NKeys/JWT via the `basis-company/nats` options).
- Restrict Samsa's user to the `samsa.>` subjects, `$JS.API.>` for its streams, and the KV bucket.
- Enable TLS for any connection that leaves the host.

## Troubleshooting

| Symptom | Fix |
|---|---|
| `nats: JetStream not enabled` | Start the server with `--jetstream` |
| Records appear out of order for one key | Two pullers on one partition: make sure every worker uses Samsa (not a manual NATS consumer with the same durable name) |
| Lag never decreases | Check `ack_wait` > handler time; look at `jsz` for ack-pending |
| `insufficient resources` | JetStream storage limits reached: raise `max_file_store` on the server or tighten retention |
