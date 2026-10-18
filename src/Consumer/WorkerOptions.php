<?php

namespace Siberfx\Samsa\Consumer;

use InvalidArgumentException;

final readonly class WorkerOptions
{
    /**
     * @param  list<int>|null  $partitions  only claim these partitions (disables fair share)
     */
    public function __construct(
        public int $batchSize = 100,
        public int $sleepMs = 250,
        public string $commit = 'batch',
        public string $startFrom = 'earliest',
        public int $leaseSeconds = 30,
        public int $heartbeatSeconds = 10,
        public ?int $maxPartitionsPerWorker = null,
        public ?array $partitions = null,
        public bool $once = false,
        public bool $stopWhenEmpty = false,
        public ?int $maxRecords = null,
        public ?int $maxTime = null,
        public int $memoryMb = 128,
    ) {
        if (! in_array($commit, ['batch', 'record'], true)) {
            throw new InvalidArgumentException("Invalid commit mode [{$commit}]. Use batch or record.");
        }

        if (! in_array($startFrom, ['earliest', 'latest'], true)) {
            throw new InvalidArgumentException("Invalid start position [{$startFrom}]. Use earliest or latest.");
        }

        if ($batchSize < 1) {
            throw new InvalidArgumentException('The batch size must be at least 1.');
        }

        if ($heartbeatSeconds < 1 || $heartbeatSeconds * 2 > $leaseSeconds) {
            throw new InvalidArgumentException('heartbeat_seconds must be at least 1 and at most half of lease_seconds.');
        }
    }

    /**
     * @param  array<string, mixed>  $config  the samsa.consumer config
     * @param  array<string, mixed>  $overrides
     */
    public static function fromConfig(array $config, array $overrides = []): self
    {
        $values = array_filter([
            'batchSize' => isset($config['batch_size']) ? (int) $config['batch_size'] : null,
            'sleepMs' => isset($config['poll_interval_ms']) ? (int) $config['poll_interval_ms'] : null,
            'commit' => $config['commit'] ?? null,
            'startFrom' => $config['start_from'] ?? null,
            'leaseSeconds' => isset($config['lease_seconds']) ? (int) $config['lease_seconds'] : null,
            'heartbeatSeconds' => isset($config['heartbeat_seconds']) ? (int) $config['heartbeat_seconds'] : null,
            'maxPartitionsPerWorker' => isset($config['max_partitions_per_worker']) ? (int) $config['max_partitions_per_worker'] : null,
        ], fn ($value) => $value !== null);

        return new self(...[...$values, ...array_filter($overrides, fn ($value) => $value !== null)]);
    }
}
