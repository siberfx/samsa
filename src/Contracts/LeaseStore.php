<?php

namespace Siberfx\Samsa\Contracts;

use Siberfx\Samsa\Data\LeaseInfo;

/**
 * Partition ownership within a consumer group.
 */
interface LeaseStore
{
    /**
     * Acquire or renew a lease. Returns true when $memberId owns the partition afterwards.
     */
    public function acquire(string $group, string $topic, int $partition, string $memberId, int $ttlSeconds): bool;

    public function release(string $group, string $topic, int $partition, string $memberId): void;

    /**
     * @return array<int, LeaseInfo> keyed by partition (only partitions that have a lease row)
     */
    public function leases(string $group, string $topic): array;

    public function heartbeat(string $group, string $topic, string $memberId, int $ttlSeconds): void;

    /**
     * @return list<string> active member IDs, sorted
     */
    public function members(string $group, string $topic): array;

    public function leave(string $group, string $topic, string $memberId): void;
}
