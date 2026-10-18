# Changelog

All notable changes to `siberfx/samsa` are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/).

## [Unreleased]

## [1.4.0] - 2026-10-19

### Changed

- **Requires PHP 8.3+ and Laravel 13.** Support for PHP 8.2 and Laravel 12 is dropped. Composer keeps projects on those versions on Samsa 1.3.x, which stays available.
- Development now targets Orchestra Testbench 11 and PHPUnit 12, and the `siberfx/message-bus` integration is tested in every CI job.

## [1.3.0] - 2026-10-19

### Added

- **Redis Streams driver** (`SAMSA_DRIVER=redis`, Redis 6.2+, phpredis or predis). It passes the full `StorageDriverContract`, the end-to-end tests and the multi-process tests. Each partition is a stream read by entry ID. Offsets, leases and members live in hashes and sorted sets. Leases, the fenced commit, and dedup with `XADD` are atomic Lua scripts. Retention uses approximate `MAXLEN` on produce and `XTRIM MINID` in `samsa:prune`. The queue driver, event bridge and message-bus transport all work on Redis.
- `samsa.drivers.redis.dedup_ttl` option.

### Notes

- Redis Cluster is not supported yet (the lease and commit scripts span two keys).

## [1.2.0] - 2026-10-19

### Added

- **NATS JetStream driver** (`SAMSA_DRIVER=nats`, needs `basis-company/nats`). It passes the full `StorageDriverContract` and the multi-process tests. Each topic is a stream with one subject per partition, and offsets are stream sequences. Offsets, leases (compare-and-swap), members, topic metadata, dead letters and delayed records live in a compact state stream. Native retention and `Nats-Msg-Id` dedup are used, with `samsa:prune` purging by subject when `respect_slowest_consumer` is set. The queue driver, event bridge and message-bus transport all work on NATS.
- `samsa.drivers.nats.prefix` and `duplicate_window` options (`kv_bucket` was never used and has been removed).

## [1.1.0] - 2026-10-19

### Added

- **Laravel queue driver**: `'driver' => 'samsa'` queue connections. Jobs with a `samsaKey()` run strictly in order; `release()`, `later()`, `$tries`, `$backoff`, `failed()`, `queue:clear` and the queue size methods all work. Leases are released when `queue:work` stops.
- **Event bridge**: the repeatable `#[PublishTo(topic, key:, eventType:, driver:, afterCommit:)]` attribute, the `ToSamsaPayload` contract, `samsaHeaders()`, `Samsa::publish()`, and optional forwarding of `ShouldBroadcast` events.
- **`Samsa::fake()`** with `assertProduced`, `assertProducedTimes`, `assertProducedCount`, `assertProducedWithKey`, `assertNotProduced`, `assertNothingProduced` and `produced()`. It fakes all topics or only selected ones, and captures queued jobs, bridged events and bus messages too.
- Test helpers: `Samsa::consumeUntilEmpty()`, `Samsa::redeliver()`, `Samsa::assertDeadLettered()`.
- **`siberfx/message-bus` transport** (`'driver' => 'samsa'`, Laravel 13): contiguous commits, `DelayStamp` support, `HasPartitionKey` and `PartitionKeyStamp`, message counts, and listing/finding pending messages.
- Delayed records: the `DelayedStore` contract, `Samsa::produceLater()`, `Samsa::releaseDelayed()`, the `samsa:release-delayed` command (scheduled every second when a queue connection uses Samsa), and a new `create_samsa_delayed_table` migration.

### Upgrading from 1.0

Publish and run the new migration:

```bash
php artisan vendor:publish --tag=samsa-migrations
php artisan migrate
```

### Changed

- The real manager is bound as `Siberfx\Samsa\SamsaManager`; `samsa` is now an alias of it, so `Samsa::fake()` can swap the facade without affecting commands.

## [1.0.0] - 2026-10-19

First stable release.

### Added

- Core contracts: `StorageDriver` split into `LogStore`, `OffsetStore`, `LeaseStore` and `DeadLetterStore`.
- **Database driver** (MySQL/MariaDB, PostgreSQL, SQLite): per-partition offset counters locked during produce, so offsets become visible in order without gaps; transactional produce; fenced commits; lease-based partition ownership using the database clock.
- **Memory driver** for tests and local development.
- `StorageDriverContract` trait: the shared behaviour suite every driver must pass.
- Producer: `Samsa::produce()`, `produceBatch()`, the fluent `Samsa::topic()` builder, standard headers (`message_id`, `produced_at`, `producer`), event types, dedup keys, `Samsa::afterCommit()`, a cross-connection transaction guard, and optional topic auto-creation.
- Consumer: `samsa:consume` worker with consumer groups, fair-share rebalancing, crash takeover after lease expiry, batch or per-record commits, in-place retries with backoff, the `#[Retry]` attribute, `UnrecoverableRecordException`, `retry.dont_retry`, dead letters, `BatchHandler`, `start_from`, graceful shutdown signals and `samsa:restart`.
- `OnlyOnce` idempotency helper backed by `samsa_processed`.
- Commands: `samsa:topic:create|list|alter|delete`, `samsa:produce`, `samsa:tail`, `samsa:consume`, `samsa:restart`, `samsa:status`, `samsa:groups`, `samsa:seek`, `samsa:prune`, `samsa:failed`, `samsa:failed:show|retry|forget|flush`.
- Events: `RecordProduced`, `RecordConsumed`, `RecordFailed`, `RecordDeadLettered`, `PartitionAssigned`, `PartitionRevoked`, `WorkerStopping`.
- Multi-process integration tests for concurrent producers and for workers that are killed while consuming.

[Unreleased]: https://github.com/siberfx/samsa/compare/v1.4.0...HEAD
[1.4.0]: https://github.com/siberfx/samsa/compare/v1.3.0...v1.4.0
[1.3.0]: https://github.com/siberfx/samsa/compare/v1.2.0...v1.3.0
[1.2.0]: https://github.com/siberfx/samsa/compare/v1.1.0...v1.2.0
[1.1.0]: https://github.com/siberfx/samsa/compare/v1.0.0...v1.1.0
[1.0.0]: https://github.com/siberfx/samsa/releases/tag/v1.0.0
