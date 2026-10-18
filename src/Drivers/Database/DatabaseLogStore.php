<?php

namespace Siberfx\Samsa\Drivers\Database;

use Carbon\CarbonImmutable;
use DateTimeInterface;
use Illuminate\Database\Connection;
use Illuminate\Database\Query\Builder;
use Siberfx\Samsa\Contracts\LogStore;
use Siberfx\Samsa\Contracts\Serializer;
use Siberfx\Samsa\Data\OutgoingRecord;
use Siberfx\Samsa\Data\Record;
use Siberfx\Samsa\Data\RecordMetadata;
use Siberfx\Samsa\Data\RetentionPolicy;
use Siberfx\Samsa\Data\TopicConfig;
use Siberfx\Samsa\Exceptions\InvalidTopicException;
use Siberfx\Samsa\Exceptions\TopicNotFoundException;

final class DatabaseLogStore implements LogStore
{
    private const PRUNE_CHUNK = 5000;

    private const INSERT_CHUNK = 500;

    public function __construct(
        private readonly Connection $connection,
        private readonly Tables $tables,
        private readonly Serializer $serializer,
    ) {}

    public function ensureTopic(TopicConfig $topic): TopicConfig
    {
        return $this->connection->transaction(function () use ($topic) {
            $existing = $this->topic($topic->name);
            $now = CarbonImmutable::now('UTC');

            if ($existing !== null) {
                $this->connection->table($this->tables->topics)->where('name', $topic->name)->update([
                    'retention' => $this->serializer->encode($topic->retention->toArray()),
                    'updated_at' => $now,
                ]);

                return new TopicConfig($topic->name, $existing->partitions, $topic->retention);
            }

            $this->connection->table($this->tables->topics)->insertOrIgnore([
                'name' => $topic->name,
                'partitions' => $topic->partitions,
                'retention' => $this->serializer->encode($topic->retention->toArray()),
                'created_at' => $now,
                'updated_at' => $now,
            ]);

            $this->createPartitionRows($topic->name, 0, $topic->partitions);

            return $this->topic($topic->name) ?? $topic;
        });
    }

    public function topic(string $name): ?TopicConfig
    {
        $row = $this->connection->table($this->tables->topics)->where('name', $name)->first();

        return $row === null ? null : $this->hydrateTopic($row);
    }

    public function topics(): array
    {
        return $this->connection->table($this->tables->topics)
            ->orderBy('name')
            ->get()
            ->map(fn (object $row) => $this->hydrateTopic($row))
            ->values()
            ->all();
    }

    public function growPartitions(string $topic, int $partitions): TopicConfig
    {
        return $this->connection->transaction(function () use ($topic, $partitions) {
            $config = $this->topic($topic) ?? throw TopicNotFoundException::named($topic);

            if ($partitions < $config->partitions) {
                throw InvalidTopicException::cannotShrink($topic, $config->partitions, $partitions);
            }

            if ($partitions === $config->partitions) {
                return $config;
            }

            $this->createPartitionRows($topic, $config->partitions, $partitions);

            $this->connection->table($this->tables->topics)->where('name', $topic)->update([
                'partitions' => $partitions,
                'updated_at' => CarbonImmutable::now('UTC'),
            ]);

            return new TopicConfig($topic, $partitions, $config->retention);
        });
    }

    public function deleteTopic(string $topic): void
    {
        $this->connection->transaction(function () use ($topic) {
            foreach ([$this->tables->log, $this->tables->partitions, $this->tables->offsets, $this->tables->leases, $this->tables->members, $this->tables->failed] as $table) {
                $this->connection->table($table)->where('topic', $topic)->delete();
            }

            $this->connection->table($this->tables->topics)->where('name', $topic)->delete();
        });
    }

    public function append(string $topic, int $partition, OutgoingRecord $record): RecordMetadata
    {
        return $this->appendBatch($topic, $partition, [$record])[0];
    }

    public function appendBatch(string $topic, int $partition, array $records): array
    {
        if ($records === []) {
            return [];
        }

        return $this->connection->transaction(function () use ($topic, $partition, $records) {
            // The row lock serializes producers of this partition until commit, so
            // offsets become visible in order and without gaps.
            $counter = $this->connection->table($this->tables->partitions)
                ->where('topic', $topic)
                ->where('partition', $partition)
                ->lockForUpdate()
                ->first();

            if ($counter === null) {
                $this->connection->table($this->tables->topics)->where('name', $topic)->exists()
                    ? throw InvalidTopicException::invalidPartition($topic, $partition)
                    : throw TopicNotFoundException::named($topic);
            }

            $existing = $this->existingDedupKeys($topic, $records);
            $next = (int) $counter->next_offset;
            $now = CarbonImmutable::now('UTC');
            $rows = [];
            $metadata = [];

            foreach ($records as $record) {
                if ($record->dedupKey !== null && isset($existing[$record->dedupKey])) {
                    $metadata[] = $existing[$record->dedupKey];

                    continue;
                }

                $rows[] = [
                    'topic' => $topic,
                    'partition' => $partition,
                    'offset' => $next,
                    'key' => $record->key,
                    'dedup_key' => $record->dedupKey,
                    'event_type' => $record->eventType,
                    'payload' => $this->serializer->encode($record->payload),
                    'headers' => $this->serializer->encode($record->headers),
                    'created_at' => DatabaseClock::format($now),
                ];

                $metadata[] = $meta = new RecordMetadata($topic, $partition, (string) $next);

                if ($record->dedupKey !== null) {
                    $existing[$record->dedupKey] = $meta;
                }

                $next++;
            }

            foreach (array_chunk($rows, self::INSERT_CHUNK) as $chunk) {
                $this->connection->table($this->tables->log)->insert($chunk);
            }

            if ($rows !== []) {
                $this->connection->table($this->tables->partitions)
                    ->where('topic', $topic)
                    ->where('partition', $partition)
                    ->update(['next_offset' => $next]);
            }

            return $metadata;
        });
    }

    public function read(string $topic, int $partition, ?string $afterOffset, int $limit): array
    {
        return $this->connection->table($this->tables->log)
            ->where('topic', $topic)
            ->where('partition', $partition)
            ->when($afterOffset !== null, fn (Builder $q) => $q->where('offset', '>', (int) $afterOffset))
            ->orderBy('offset')
            ->limit($limit)
            ->get()
            ->map(fn (object $row) => new Record(
                topic: $topic,
                partition: $partition,
                offset: (string) $row->offset,
                key: $row->key,
                payload: $this->serializer->decode((string) $row->payload),
                headers: $row->headers === null ? [] : $this->serializer->decode((string) $row->headers),
                createdAt: CarbonImmutable::parse((string) $row->created_at, 'UTC'),
                eventType: $row->event_type,
            ))
            ->values()
            ->all();
    }

    public function latestOffset(string $topic, int $partition): ?string
    {
        $next = $this->connection->table($this->tables->partitions)
            ->where('topic', $topic)
            ->where('partition', $partition)
            ->value('next_offset');

        if ($next === null) {
            throw TopicNotFoundException::named($topic);
        }

        return (int) $next === 0 ? null : (string) ((int) $next - 1);
    }

    public function offsetBeforeTime(string $topic, int $partition, DateTimeInterface $time): ?string
    {
        $offset = $this->connection->table($this->tables->log)
            ->where('topic', $topic)
            ->where('partition', $partition)
            ->where('created_at', '<', DatabaseClock::format(CarbonImmutable::instance($time)))
            ->max('offset');

        return $offset === null ? null : (string) $offset;
    }

    public function offsetBefore(string $topic, int $partition, string $offset): ?string
    {
        return (int) $offset <= 0 ? null : (string) ((int) $offset - 1);
    }

    public function countAfter(string $topic, int $partition, ?string $afterOffset): int
    {
        return $this->connection->table($this->tables->log)
            ->where('topic', $topic)
            ->where('partition', $partition)
            ->when($afterOffset !== null, fn (Builder $q) => $q->where('offset', '>', (int) $afterOffset))
            ->count();
    }

    public function prune(string $topic, RetentionPolicy $policy, ?array $protectAfter = null): int
    {
        $config = $this->topic($topic) ?? throw TopicNotFoundException::named($topic);

        if ($policy->maxAgeSeconds === null && $policy->maxRecordsPerPartition === null) {
            return 0;
        }

        $cutoff = $policy->maxAgeSeconds === null
            ? null
            : DatabaseClock::format(CarbonImmutable::now('UTC')->subSeconds($policy->maxAgeSeconds));

        $deleted = 0;

        for ($partition = 0; $partition < $config->partitions; $partition++) {
            $keepFrom = null;

            if ($policy->maxRecordsPerPartition !== null) {
                $latest = $this->latestOffset($topic, $partition);
                $keepFrom = $latest === null ? null : (int) $latest + 1 - $policy->maxRecordsPerPartition;
            }

            $protected = null;

            if ($protectAfter !== null) {
                $value = $protectAfter[$partition] ?? null;
                $protected = $value === null ? -1 : (int) $value;
            }

            $deleted += $this->prunePartition($topic, $partition, $cutoff, $keepFrom, $protected);
        }

        return $deleted;
    }

    private function prunePartition(string $topic, int $partition, ?string $cutoff, ?int $keepFrom, ?int $protected): int
    {
        if ($protected === -1) {
            return 0;
        }

        $deleted = 0;

        $candidates = fn () => $this->connection->table($this->tables->log)
            ->where('topic', $topic)
            ->where('partition', $partition)
            ->where(function (Builder $q) use ($cutoff, $keepFrom) {
                if ($cutoff !== null) {
                    $q->orWhere('created_at', '<', $cutoff);
                }

                if ($keepFrom !== null) {
                    $q->orWhere('offset', '<', $keepFrom);
                }
            })
            ->when($protected !== null, fn (Builder $q) => $q->where('offset', '<=', $protected));

        do {
            $offsets = $candidates()->orderBy('offset')->limit(self::PRUNE_CHUNK)->pluck('offset');

            if ($offsets->isEmpty()) {
                break;
            }

            $count = $candidates()->whereBetween('offset', [(int) $offsets->first(), (int) $offsets->last()])->delete();
            $deleted += $count;
        } while ($offsets->count() === self::PRUNE_CHUNK);

        return $deleted;
    }

    private function createPartitionRows(string $topic, int $from, int $to): void
    {
        $rows = [];

        for ($partition = $from; $partition < $to; $partition++) {
            $rows[] = ['topic' => $topic, 'partition' => $partition, 'next_offset' => 0];
        }

        foreach (array_chunk($rows, self::INSERT_CHUNK) as $chunk) {
            $this->connection->table($this->tables->partitions)->insertOrIgnore($chunk);
        }
    }

    /**
     * @param  list<OutgoingRecord>  $records
     * @return array<string, RecordMetadata>
     */
    private function existingDedupKeys(string $topic, array $records): array
    {
        $keys = array_values(array_unique(array_filter(array_map(fn (OutgoingRecord $r) => $r->dedupKey, $records), fn ($k) => $k !== null)));

        if ($keys === []) {
            return [];
        }

        return $this->connection->table($this->tables->log)
            ->where('topic', $topic)
            ->whereIn('dedup_key', $keys)
            ->get(['dedup_key', 'partition', 'offset'])
            ->mapWithKeys(fn (object $row) => [
                (string) $row->dedup_key => new RecordMetadata($topic, (int) $row->partition, (string) $row->offset),
            ])
            ->all();
    }

    private function hydrateTopic(object $row): TopicConfig
    {
        $retention = $row->retention === null ? [] : $this->serializer->decode((string) $row->retention);

        return new TopicConfig((string) $row->name, (int) $row->partitions, RetentionPolicy::fromArray($retention));
    }
}
