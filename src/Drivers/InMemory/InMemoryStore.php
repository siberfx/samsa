<?php

namespace Siberfx\Samsa\Drivers\InMemory;

use Carbon\CarbonImmutable;
use DateTimeInterface;
use Illuminate\Support\Str;
use Siberfx\Samsa\Contracts\DeadLetterStore;
use Siberfx\Samsa\Contracts\LeaseStore;
use Siberfx\Samsa\Contracts\LogStore;
use Siberfx\Samsa\Contracts\OffsetStore;
use Siberfx\Samsa\Contracts\Serializer;
use Siberfx\Samsa\Data\FailedRecord;
use Siberfx\Samsa\Data\LeaseInfo;
use Siberfx\Samsa\Data\OutgoingRecord;
use Siberfx\Samsa\Data\Record;
use Siberfx\Samsa\Data\RecordMetadata;
use Siberfx\Samsa\Data\RetentionPolicy;
use Siberfx\Samsa\Data\TopicConfig;
use Siberfx\Samsa\Exceptions\InvalidTopicException;
use Siberfx\Samsa\Exceptions\LeaseLostException;
use Siberfx\Samsa\Exceptions\TopicNotFoundException;
use Siberfx\Samsa\Serialization\JsonSerializer;
use Throwable;

/**
 * Process-local storage behind InMemoryDriver. State lives as long as the instance.
 */
final class InMemoryStore implements DeadLetterStore, LeaseStore, LogStore, OffsetStore
{
    /** @var array<string, TopicConfig> */
    private array $topics = [];

    /** @var array<string, array<int, int>> topic => partition => next offset */
    private array $nextOffsets = [];

    /** @var array<string, array<int, list<array{offset: int, key: ?string, dedup_key: ?string, event_type: ?string, payload: string, headers: string, created_at: CarbonImmutable}>>> */
    private array $log = [];

    /** @var array<string, array<string, array<int, ?int>>> group => topic => partition => committed */
    private array $offsets = [];

    /** @var array<string, array{member: string, until: CarbonImmutable}> */
    private array $leases = [];

    /** @var array<string, CarbonImmutable> */
    private array $members = [];

    /** @var array<string, FailedRecord> */
    private array $failed = [];

    private Serializer $serializer;

    public function __construct(?Serializer $serializer = null)
    {
        $this->serializer = $serializer ?? new JsonSerializer;
    }

    // ---------------------------------------------------------------- LogStore

    public function ensureTopic(TopicConfig $topic): TopicConfig
    {
        $existing = $this->topics[$topic->name] ?? null;

        if ($existing !== null) {
            return $this->topics[$topic->name] = new TopicConfig($topic->name, $existing->partitions, $topic->retention);
        }

        $this->topics[$topic->name] = $topic;

        for ($p = 0; $p < $topic->partitions; $p++) {
            $this->nextOffsets[$topic->name][$p] = 0;
            $this->log[$topic->name][$p] = [];
        }

        return $topic;
    }

    public function topic(string $name): ?TopicConfig
    {
        return $this->topics[$name] ?? null;
    }

    public function topics(): array
    {
        $topics = $this->topics;
        ksort($topics);

        return array_values($topics);
    }

    public function growPartitions(string $topic, int $partitions): TopicConfig
    {
        $config = $this->requireTopic($topic);

        if ($partitions < $config->partitions) {
            throw InvalidTopicException::cannotShrink($topic, $config->partitions, $partitions);
        }

        for ($p = $config->partitions; $p < $partitions; $p++) {
            $this->nextOffsets[$topic][$p] = 0;
            $this->log[$topic][$p] = [];
        }

        return $this->topics[$topic] = new TopicConfig($topic, $partitions, $config->retention);
    }

    public function deleteTopic(string $topic): void
    {
        unset($this->topics[$topic], $this->nextOffsets[$topic], $this->log[$topic]);

        foreach ($this->offsets as $group => $topics) {
            unset($this->offsets[$group][$topic]);
        }

        foreach (array_keys($this->leases) as $key) {
            if (str_contains($key, "|{$topic}|")) {
                unset($this->leases[$key]);
            }
        }
    }

    public function append(string $topic, int $partition, OutgoingRecord $record): RecordMetadata
    {
        return $this->appendBatch($topic, $partition, [$record])[0];
    }

    public function appendBatch(string $topic, int $partition, array $records): array
    {
        $this->requirePartition($topic, $partition);

        $metadata = [];

        foreach ($records as $record) {
            if ($record->dedupKey !== null && ($existing = $this->findByDedupKey($topic, $record->dedupKey)) !== null) {
                $metadata[] = $existing;

                continue;
            }

            $offset = $this->nextOffsets[$topic][$partition]++;

            $this->log[$topic][$partition][] = [
                'offset' => $offset,
                'key' => $record->key,
                'dedup_key' => $record->dedupKey,
                'event_type' => $record->eventType,
                'payload' => $this->serializer->encode($record->payload),
                'headers' => $this->serializer->encode($record->headers),
                'created_at' => CarbonImmutable::now(),
            ];

            $metadata[] = new RecordMetadata($topic, $partition, (string) $offset);
        }

        return $metadata;
    }

    public function read(string $topic, int $partition, ?string $afterOffset, int $limit): array
    {
        $this->requirePartition($topic, $partition);

        $after = $afterOffset === null ? -1 : (int) $afterOffset;
        $records = [];

        foreach ($this->log[$topic][$partition] as $row) {
            if ($row['offset'] <= $after) {
                continue;
            }

            $records[] = new Record(
                topic: $topic,
                partition: $partition,
                offset: (string) $row['offset'],
                key: $row['key'],
                payload: $this->serializer->decode($row['payload']),
                headers: $this->serializer->decode($row['headers']),
                createdAt: $row['created_at'],
                eventType: $row['event_type'],
            );

            if (count($records) >= $limit) {
                break;
            }
        }

        return $records;
    }

    public function latestOffset(string $topic, int $partition): ?string
    {
        $this->requirePartition($topic, $partition);

        $last = end($this->log[$topic][$partition]);

        return $last === false ? null : (string) $last['offset'];
    }

    public function offsetBeforeTime(string $topic, int $partition, DateTimeInterface $time): ?string
    {
        $this->requirePartition($topic, $partition);

        $before = null;

        foreach ($this->log[$topic][$partition] as $row) {
            if ($row['created_at']->greaterThanOrEqualTo($time)) {
                break;
            }

            $before = (string) $row['offset'];
        }

        return $before;
    }

    public function offsetBefore(string $topic, int $partition, string $offset): ?string
    {
        return (int) $offset <= 0 ? null : (string) ((int) $offset - 1);
    }

    public function countAfter(string $topic, int $partition, ?string $afterOffset): int
    {
        $this->requirePartition($topic, $partition);

        $after = $afterOffset === null ? -1 : (int) $afterOffset;

        return count(array_filter($this->log[$topic][$partition], fn (array $row) => $row['offset'] > $after));
    }

    public function prune(string $topic, RetentionPolicy $policy, ?array $protectAfter = null): int
    {
        $config = $this->requireTopic($topic);
        $deleted = 0;
        $cutoff = $policy->maxAgeSeconds === null ? null : CarbonImmutable::now()->subSeconds($policy->maxAgeSeconds);

        for ($p = 0; $p < $config->partitions; $p++) {
            $rows = $this->log[$topic][$p];
            $keepFrom = $policy->maxRecordsPerPartition === null ? null : $this->nextOffsets[$topic][$p] - $policy->maxRecordsPerPartition;
            $protected = $protectAfter === null ? PHP_INT_MAX : (array_key_exists($p, $protectAfter) && $protectAfter[$p] !== null ? (int) $protectAfter[$p] : -1);

            $kept = array_values(array_filter($rows, function (array $row) use ($cutoff, $keepFrom, $protected) {
                $expired = ($cutoff !== null && $row['created_at']->lessThan($cutoff))
                    || ($keepFrom !== null && $row['offset'] < $keepFrom);

                return ! $expired || $row['offset'] > $protected;
            }));

            $deleted += count($rows) - count($kept);
            $this->log[$topic][$p] = $kept;
        }

        return $deleted;
    }

    // ------------------------------------------------------------- OffsetStore

    public function committed(string $group, string $topic, int $partition): ?string
    {
        $offset = $this->offsets[$group][$topic][$partition] ?? null;

        return $offset === null ? null : (string) $offset;
    }

    public function hasPosition(string $group, string $topic, int $partition): bool
    {
        return isset($this->offsets[$group][$topic]) && array_key_exists($partition, $this->offsets[$group][$topic]);
    }

    public function commit(string $group, string $topic, int $partition, string $offset, ?string $memberId = null): void
    {
        if ($memberId !== null && ! $this->owns($group, $topic, $partition, $memberId)) {
            throw LeaseLostException::for($group, $topic, $partition, $memberId);
        }

        $this->offsets[$group][$topic][$partition] = (int) $offset;
    }

    public function seek(string $group, string $topic, int $partition, ?string $offset): void
    {
        $this->offsets[$group][$topic][$partition] = $offset === null ? null : (int) $offset;
    }

    public function groups(string $topic): array
    {
        $groups = array_keys(array_filter($this->offsets, fn (array $topics) => isset($topics[$topic])));
        sort($groups);

        return array_map('strval', $groups);
    }

    public function allGroups(): array
    {
        $groups = [];

        foreach ($this->offsets as $group => $topics) {
            $names = array_map('strval', array_keys($topics));
            sort($names);
            $groups[(string) $group] = $names;
        }

        ksort($groups);

        return $groups;
    }

    // -------------------------------------------------------------- LeaseStore

    public function acquire(string $group, string $topic, int $partition, string $memberId, int $ttlSeconds): bool
    {
        $key = "{$group}|{$topic}|{$partition}";
        $now = CarbonImmutable::now();
        $lease = $this->leases[$key] ?? null;

        if ($lease !== null && $lease['member'] !== $memberId && $lease['until']->greaterThan($now)) {
            return false;
        }

        $this->leases[$key] = ['member' => $memberId, 'until' => $now->addSeconds($ttlSeconds)];

        return true;
    }

    public function release(string $group, string $topic, int $partition, string $memberId): void
    {
        $key = "{$group}|{$topic}|{$partition}";

        if (($this->leases[$key]['member'] ?? null) === $memberId) {
            unset($this->leases[$key]);
        }
    }

    public function leases(string $group, string $topic): array
    {
        $leases = [];

        foreach ($this->leases as $key => $lease) {
            [$g, $t, $p] = explode('|', $key);

            if ($g === $group && $t === $topic) {
                $leases[(int) $p] = new LeaseInfo((int) $p, $lease['member'], $lease['until']);
            }
        }

        ksort($leases);

        return $leases;
    }

    public function heartbeat(string $group, string $topic, string $memberId, int $ttlSeconds): void
    {
        $this->members["{$group}|{$topic}|{$memberId}"] = CarbonImmutable::now()->addSeconds($ttlSeconds);
    }

    public function members(string $group, string $topic): array
    {
        $now = CarbonImmutable::now();
        $members = [];

        foreach ($this->members as $key => $expiresAt) {
            [$g, $t, $member] = explode('|', $key, 3);

            if ($g === $group && $t === $topic && $expiresAt->greaterThan($now)) {
                $members[] = $member;
            }
        }

        sort($members);

        return $members;
    }

    public function leave(string $group, string $topic, string $memberId): void
    {
        unset($this->members["{$group}|{$topic}|{$memberId}"]);
    }

    // --------------------------------------------------------- DeadLetterStore

    public function store(string $group, Record $record, Throwable $error, int $attempts): FailedRecord
    {
        $failed = new FailedRecord(
            id: (string) Str::uuid(),
            group: $group,
            record: $record,
            attempts: $attempts,
            exception: (string) $error,
            failedAt: CarbonImmutable::now(),
        );

        return $this->failed[$failed->id] = $failed;
    }

    public function find(string $id): ?FailedRecord
    {
        return $this->failed[$id] ?? null;
    }

    public function all(?string $group = null, ?string $topic = null, int $limit = 100): array
    {
        $matching = array_filter($this->failed, fn (FailedRecord $f) => ($group === null || $f->group === $group)
            && ($topic === null || $f->record->topic === $topic));

        return array_slice(array_values($matching), 0, $limit);
    }

    public function forget(string $id): bool
    {
        $exists = isset($this->failed[$id]);
        unset($this->failed[$id]);

        return $exists;
    }

    public function flush(?DateTimeInterface $before = null): int
    {
        $count = 0;

        foreach ($this->failed as $id => $failed) {
            if ($before === null || $failed->failedAt->lessThan($before)) {
                unset($this->failed[$id]);
                $count++;
            }
        }

        return $count;
    }

    // ----------------------------------------------------------------- helpers

    private function owns(string $group, string $topic, int $partition, string $memberId): bool
    {
        $lease = $this->leases["{$group}|{$topic}|{$partition}"] ?? null;

        return $lease !== null && $lease['member'] === $memberId && $lease['until']->greaterThan(CarbonImmutable::now());
    }

    private function findByDedupKey(string $topic, string $dedupKey): ?RecordMetadata
    {
        foreach ($this->log[$topic] as $partition => $rows) {
            foreach ($rows as $row) {
                if ($row['dedup_key'] === $dedupKey) {
                    return new RecordMetadata($topic, $partition, (string) $row['offset']);
                }
            }
        }

        return null;
    }

    private function requireTopic(string $topic): TopicConfig
    {
        return $this->topics[$topic] ?? throw TopicNotFoundException::named($topic);
    }

    private function requirePartition(string $topic, int $partition): void
    {
        $config = $this->requireTopic($topic);

        if ($partition < 0 || $partition >= $config->partitions) {
            throw InvalidTopicException::invalidPartition($topic, $partition);
        }
    }
}
