<?php

namespace Siberfx\Samsa\Drivers\Database;

use Carbon\CarbonImmutable;
use DateTimeInterface;
use Illuminate\Database\Connection;
use Illuminate\Support\Str;
use Siberfx\Samsa\Contracts\DeadLetterStore;
use Siberfx\Samsa\Contracts\Serializer;
use Siberfx\Samsa\Data\FailedRecord;
use Siberfx\Samsa\Data\Record;
use Throwable;

final class DatabaseDeadLetterStore implements DeadLetterStore
{
    public function __construct(
        private readonly Connection $connection,
        private readonly Tables $tables,
        private readonly Serializer $serializer,
    ) {}

    public function store(string $group, Record $record, Throwable $error, int $attempts): FailedRecord
    {
        $failed = new FailedRecord(
            id: (string) Str::uuid(),
            group: $group,
            record: $record,
            attempts: $attempts,
            exception: (string) $error,
            failedAt: CarbonImmutable::now('UTC'),
        );

        $this->connection->table($this->tables->failed)->insert([
            'id' => $failed->id,
            'group_name' => $group,
            'topic' => $record->topic,
            'partition' => $record->partition,
            'offset' => (int) $record->offset,
            'key' => $record->key,
            'event_type' => $record->eventType,
            'payload' => $this->serializer->encode($record->payload),
            'headers' => $this->serializer->encode($record->headers),
            'record_created_at' => DatabaseClock::format($record->createdAt),
            'attempts' => $attempts,
            'exception' => $failed->exception,
            'failed_at' => DatabaseClock::format($failed->failedAt),
        ]);

        return $failed;
    }

    public function find(string $id): ?FailedRecord
    {
        if (! Str::isUuid($id)) {
            return null;
        }

        $row = $this->connection->table($this->tables->failed)->where('id', $id)->first();

        return $row === null ? null : $this->hydrate($row);
    }

    public function all(?string $group = null, ?string $topic = null, int $limit = 100): array
    {
        return $this->connection->table($this->tables->failed)
            ->when($group !== null, fn ($q) => $q->where('group_name', $group))
            ->when($topic !== null, fn ($q) => $q->where('topic', $topic))
            ->orderBy('failed_at')
            ->limit($limit)
            ->get()
            ->map(fn (object $row) => $this->hydrate($row))
            ->values()
            ->all();
    }

    public function forget(string $id): bool
    {
        if (! Str::isUuid($id)) {
            return false;
        }

        return $this->connection->table($this->tables->failed)->where('id', $id)->delete() > 0;
    }

    public function flush(?DateTimeInterface $before = null): int
    {
        return $this->connection->table($this->tables->failed)
            ->when($before !== null, fn ($q) => $q->where('failed_at', '<', DatabaseClock::format(CarbonImmutable::instance($before))))
            ->delete();
    }

    private function hydrate(object $row): FailedRecord
    {
        return new FailedRecord(
            id: (string) $row->id,
            group: (string) $row->group_name,
            record: new Record(
                topic: (string) $row->topic,
                partition: (int) $row->partition,
                offset: (string) $row->offset,
                key: $row->key,
                payload: $this->serializer->decode((string) $row->payload),
                headers: $row->headers === null ? [] : $this->serializer->decode((string) $row->headers),
                createdAt: CarbonImmutable::parse((string) $row->record_created_at, 'UTC'),
                eventType: $row->event_type,
            ),
            attempts: (int) $row->attempts,
            exception: (string) $row->exception,
            failedAt: CarbonImmutable::parse((string) $row->failed_at, 'UTC'),
        );
    }
}
