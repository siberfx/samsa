# NATS JetStream Driver

[NATS JetStream](https://docs.nats.io/nats-concepts/jetstream) is a persistent streaming layer built into the NATS server: a single ~20 MB binary with optional clustering and replication. It takes the log off your application database and adds replication, and it's the driver to choose for high throughput.

- [Why use it](#why-use-it)
- [Run a NATS server](#run-a-nats-server)
- [Install and configure](#install-and-configure)
- [How Samsa maps to JetStream](#how-samsa-maps-to-jetstream)
- [Offsets](#offsets)
- [Groups, leases and commits](#groups-leases-and-commits)
- [Deduplication](#deduplication)
- [Retention](#retention)
- [Dead letters and delayed records](#dead-letters-and-delayed-records)
- [Clustering and replication](#clustering-and-replication)
- [Performance notes](#performance-notes)
- [Monitoring](#monitoring)
- [Security](#security)
- [Troubleshooting](#troubleshooting)

## Why use it

- **Throughput and latency** well beyond a SQL table, and no load on your application database.
- **Replication** (R3/R5) in a cluster, so it survives losing a node.
- **Native retention** (`max_age`, `max_msgs_per_subject`) and **native dedup** (`Nats-Msg-Id`).
- Pure PHP: the client is [`basis-company/nats`](https://github.com/basis-company/nats.php), no extension needed.
- Every Samsa feature works the same: consumer groups, rebalancing, fencing, retries, dead letters, seek, prune, the queue driver, the event bridge and the message-bus transport.

The trade-off is one more service to run, and no transactional produce with your database (see the [relay pattern](../producing.md#guaranteed-delivery-to-nats-or-redis-the-relay-pattern)).

## Run a NATS server

Docker, single node:

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

Check it's up: `curl http://localhost:8222/healthz` → `{"status":"ok"}`. NATS 2.10 or newer is required.

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
    'host'             => env('NATS_HOST', '127.0.0.1'),
    'port'             => (int) env('NATS_PORT', 4222),
    'user'             => env('NATS_USER'),
    'pass'             => env('NATS_PASS'),
    'token'            => env('NATS_TOKEN'),
    'timeout'          => 1.0,      // seconds per request
    'reconnect'        => true,
    'storage'          => 'file',   // file | memory, for streams Samsa creates
    'replicas'         => 1,        // 3 or 5 in a JetStream cluster
    'prefix'           => env('SAMSA_NATS_PREFIX', 'samsa'),
    'duplicate_window' => 120,      // seconds
],
```

Then create topics as usual. Nothing else needs migrating:

```bash
php artisan samsa:topic:create orders --partitions=16 --driver=nats
```

You can also use NATS next to the Database driver: `Samsa::driver('nats')->produce(...)`, `samsa:consume billing orders --driver=nats`.

## How Samsa maps to JetStream

With the default prefix `samsa`:

| Samsa | JetStream |
|---|---|
| Topic `orders` | Stream `SAMSA_orders`, subjects `samsa.orders.*` |
| Partition 3 | Subject `samsa.orders.3` |
| Offset | Stream sequence number |
| Read after offset N | `STREAM.MSG.GET` with `seq: N + 1` and `next_by_subj` |
| Key, event type, headers, created-at | Message headers `Samsa-Key`, `Samsa-Event-Type`, `Samsa-Headers`, `Samsa-Created-At` |
| Dedup key | `Nats-Msg-Id` header |
| Topic metadata, committed offsets, leases, members, dead letters, delayed records | Entries in the state stream `SAMSA__STATE` (subjects `samsa-state.>`, one message kept per subject) |

Topic names that aren't plain `[A-Za-z0-9_-]` (for example `orders.eu`) are hex-encoded in subjects and stream names (`samsa.~6f72646572732e6575.3`), so they never clash with NATS wildcards.

Change `prefix` to run several independent Samsa installations on one NATS account.

## Offsets

Offsets are **stream sequences**. Within a partition they always increase, but they are **not contiguous** (`3, 7, 8, 12…`), because all partitions of a topic share one stream. That's expected. Use `samsa:status` and `Samsa::stats()` for lag rather than subtracting offsets yourself.

## Groups, leases and commits

Samsa doesn't use JetStream's own consumer groups for delivery. It reads by sequence and keeps the group position itself, exactly like the other drivers. That's what keeps per-key ordering, seek, replay and fencing identical across drivers:

- **Committed offsets** are state entries (`offsets.{group}.{topic}.{partition}`).
- **Leases** are state entries updated with compare-and-swap (`Nats-Expected-Last-Subject-Sequence`). Two members can never both win a partition.
- **Fencing:** a commit is refused unless the member still holds an unexpired lease.
- Lease expiry uses the workers' clocks, so keep worker machines NTP-synced.

## Deduplication

Records with a dedup key are published with `Nats-Msg-Id`. JetStream drops duplicates within the stream's **duplicate window** (`duplicate_window`, default 120 seconds, capped at the topic's `max_age`), and Samsa then returns the original record's metadata. This is a time window, unlike the Database driver's dedup, which lasts until retention removes the original record.

## Retention

| Samsa retention | JetStream |
|---|---|
| `max_age_seconds` | Stream `max_age`, enforced by the server |
| `max_records_per_partition` | Stream `max_msgs_per_subject`, enforced by the server |
| `respect_slowest_consumer: true` | Native limits are **not** set; `samsa:prune` purges (`STREAM.PURGE` with a subject filter) only below the slowest group's commit |

`samsa:prune` works on NATS too, for example to apply a policy on demand.

`storage: memory` is faster but loses data when the server restarts. Use it for caches and tests only.

## Dead letters and delayed records

Dead letters and delayed records (`Queue::later()`, released jobs, message-bus `DelayStamp`s) are entries in the state stream, so `samsa:failed*`, `samsa:release-delayed` and the queue driver work unchanged. Releasing delayed records scans the delayed entries, so it suits moderate numbers of delayed jobs.

## Clustering and replication

Run three NATS servers with JetStream and set `'replicas' => 3`. Every topic stream and the state stream are then replicated, and the cluster survives one node failing. The client reconnects automatically (`reconnect: true`).

## Performance notes

- Producing is one publish plus a PubAck per record.
- Reading is one `STREAM.MSG.GET` request per record. Batches stay ordered and simple, at the cost of one round trip per record. On a local network that's well under a millisecond each.
- `samsa:status` and `size()` count lag with a short-lived ephemeral consumer per partition.

## Monitoring

| Endpoint | Shows |
|---|---|
| `http://nats:8222/healthz` | Liveness |
| `http://nats:8222/varz` | Server stats (connections, memory, throughput) |
| `http://nats:8222/jsz?streams=true` | Per-stream message counts and sizes |

`samsa:status --driver=nats` shows the same lag view as the other drivers. The NATS CLI (`nats stream report`) is useful too.

## Security

- Use credentials: user/pass or a token.
- Restrict Samsa's user to `{prefix}.>`, `{prefix}-state.>` and the `$JS.API.>` requests for its streams.
- Enable TLS for any connection that leaves the host.

## Troubleshooting

| Symptom | Fix |
|---|---|
| `The NATS JetStream driver needs basis-company/nats` | `composer require basis-company/nats` |
| `Processing timeout` | NATS unreachable or slow: check `NATS_HOST`, raise `timeout` |
| `JetStream not enabled` | Start the server with `--jetstream` |
| `insufficient resources` | JetStream storage limits reached: raise `max_file_store` on the server or tighten retention |
| Dedup key not honoured | The duplicate arrived after `duplicate_window`: raise it, or dedup in the handler (see [Idempotency](../idempotency.md)) |
