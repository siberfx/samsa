<?php

namespace Siberfx\Samsa\Drivers\Database;

use Illuminate\Database\Connection;
use Siberfx\Samsa\Contracts\DeadLetterStore;
use Siberfx\Samsa\Contracts\LeaseStore;
use Siberfx\Samsa\Contracts\LogStore;
use Siberfx\Samsa\Contracts\OffsetStore;
use Siberfx\Samsa\Contracts\Serializer;
use Siberfx\Samsa\Contracts\StorageDriver;

final class DatabaseDriver implements StorageDriver
{
    private readonly DatabaseLogStore $log;

    private readonly DatabaseOffsetStore $offsets;

    private readonly DatabaseLeaseStore $leases;

    private readonly DatabaseDeadLetterStore $deadLetters;

    public function __construct(
        private readonly Connection $connection,
        Serializer $serializer,
        string $tablePrefix = '',
    ) {
        $tables = new Tables($tablePrefix);
        $clock = new DatabaseClock($connection);

        $this->log = new DatabaseLogStore($connection, $tables, $serializer);
        $this->offsets = new DatabaseOffsetStore($connection, $tables, $clock);
        $this->leases = new DatabaseLeaseStore($connection, $tables, $clock);
        $this->deadLetters = new DatabaseDeadLetterStore($connection, $tables, $serializer);
    }

    public function connection(): Connection
    {
        return $this->connection;
    }

    public function log(): LogStore
    {
        return $this->log;
    }

    public function offsets(): OffsetStore
    {
        return $this->offsets;
    }

    public function leases(): LeaseStore
    {
        return $this->leases;
    }

    public function deadLetters(): DeadLetterStore
    {
        return $this->deadLetters;
    }
}
