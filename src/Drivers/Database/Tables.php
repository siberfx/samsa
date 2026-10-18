<?php

namespace Siberfx\Samsa\Drivers\Database;

/**
 * Table names, with the configured prefix applied.
 */
final readonly class Tables
{
    public string $topics;

    public string $partitions;

    public string $log;

    public string $offsets;

    public string $leases;

    public string $members;

    public string $failed;

    public string $processed;

    public string $delayed;

    public function __construct(string $prefix = '')
    {
        $this->topics = $prefix.'samsa_topics';
        $this->partitions = $prefix.'samsa_partitions';
        $this->log = $prefix.'samsa_log';
        $this->offsets = $prefix.'samsa_offsets';
        $this->leases = $prefix.'samsa_leases';
        $this->members = $prefix.'samsa_members';
        $this->failed = $prefix.'samsa_failed';
        $this->processed = $prefix.'samsa_processed';
        $this->delayed = $prefix.'samsa_delayed';
    }
}
