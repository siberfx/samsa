<?php

namespace Siberfx\Samsa\Contracts;

use DateTimeInterface;
use Siberfx\Samsa\Data\OutgoingRecord;
use Siberfx\Samsa\Data\Record;
use Siberfx\Samsa\Data\RecordMetadata;
use Siberfx\Samsa\Data\RetentionPolicy;
use Siberfx\Samsa\Data\TopicConfig;

/**
 * Append-only, partitioned storage. Offsets are opaque strings: only the
 * driver compares them.
 */
interface LogStore
{
    /**
     * Create the topic if it does not exist, otherwise update its retention.
     * The partition count of an existing topic is never changed here.
     */
    public function ensureTopic(TopicConfig $topic): TopicConfig;

    public function topic(string $name): ?TopicConfig;

    /**
     * @return list<TopicConfig>
     */
    public function topics(): array;

    /**
     * Increase a topic's partition count. Never shrinks.
     */
    public function growPartitions(string $topic, int $partitions): TopicConfig;

    /**
     * Delete a topic, its records and every group position on it.
     */
    public function deleteTopic(string $topic): void;

    public function append(string $topic, int $partition, OutgoingRecord $record): RecordMetadata;

    /**
     * Append records to one partition, preserving their order.
     *
     * @param  list<OutgoingRecord>  $records
     * @return list<RecordMetadata>
     */
    public function appendBatch(string $topic, int $partition, array $records): array;

    /**
     * Read up to $limit records strictly after $afterOffset (null = from the beginning).
     *
     * @return list<Record>
     */
    public function read(string $topic, int $partition, ?string $afterOffset, int $limit): array;

    public function latestOffset(string $topic, int $partition): ?string;

    /**
     * The offset just before the first record created at or after $time, or null
     * when reading from the beginning would be equivalent.
     */
    public function offsetBeforeTime(string $topic, int $partition, DateTimeInterface $time): ?string;

    /**
     * The offset just before $offset, or null when $offset is the first one.
     */
    public function offsetBefore(string $topic, int $partition, string $offset): ?string;

    /**
     * Number of records after $afterOffset.
     */
    public function countAfter(string $topic, int $partition, ?string $afterOffset): int;

    /**
     * Delete records according to the policy, never deleting anything after
     * $protectAfter (per partition) when provided. Returns the deleted count.
     *
     * @param  array<int, string|null>|null  $protectAfter  partition => last offset that may be deleted
     */
    public function prune(string $topic, RetentionPolicy $policy, ?array $protectAfter = null): int;
}
