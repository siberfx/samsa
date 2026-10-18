# Changelog

All notable changes to `siberfx/samsa` are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/).

## [Unreleased]

### Planned

- 1.2: NATS JetStream driver.
- 1.3: Redis Streams driver.

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

[Unreleased]: https://github.com/siberfx/samsa/compare/v1.1.0...HEAD
[1.1.0]: https://github.com/siberfx/samsa/compare/v1.0.0...v1.1.0
[1.0.0]: https://github.com/siberfx/samsa/releases/tag/v1.0.0
