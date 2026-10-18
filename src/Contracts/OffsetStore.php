<?php

namespace Siberfx\Samsa\Contracts;

use Siberfx\Samsa\Exceptions\LeaseLostException;

/**
 * Committed positions per (group, topic, partition). A committed offset is the
 * offset of the last record the group finished; null means "from the beginning".
 */
interface OffsetStore
{
    public function committed(string $group, string $topic, int $partition): ?string;

    /**
     * Whether the group has a stored position for the partition at all.
     */
    public function hasPosition(string $group, string $topic, int $partition): bool;

    /**
     * Commit an offset. When $memberId is given the commit is fenced: it only
     * succeeds while that member holds the partition's lease.
     *
     * @throws LeaseLostException
     */
    public function commit(string $group, string $topic, int $partition, string $offset, ?string $memberId = null): void;

    /**
     * Set a group's position without fencing (replay / skip). null = beginning.
     */
    public function seek(string $group, string $topic, int $partition, ?string $offset): void;

    /**
     * @return list<string> groups with a stored position on the topic
     */
    public function groups(string $topic): array;

    /**
     * @return array<string, list<string>> group => topics, for every known group
     */
    public function allGroups(): array;
}
