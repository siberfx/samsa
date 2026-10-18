<?php

namespace Siberfx\Samsa\Contracts;

use DateTimeInterface;
use Siberfx\Samsa\Data\FailedRecord;
use Siberfx\Samsa\Data\Record;
use Throwable;

interface DeadLetterStore
{
    public function store(string $group, Record $record, Throwable $error, int $attempts): FailedRecord;

    public function find(string $id): ?FailedRecord;

    /**
     * @return list<FailedRecord>
     */
    public function all(?string $group = null, ?string $topic = null, int $limit = 100): array;

    public function forget(string $id): bool;

    /**
     * Delete dead letters, optionally only those older than $before. Returns the deleted count.
     */
    public function flush(?DateTimeInterface $before = null): int;
}
