# Changelog

All notable changes to `siberfx/samsa` are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/) (until `1.0.0`, minor versions may break).

## [Unreleased]

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
