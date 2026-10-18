<?php

namespace Siberfx\Samsa\Consumer;

use Carbon\CarbonImmutable;
use Siberfx\Samsa\Contracts\StorageDriver;
use Siberfx\Samsa\Exceptions\TopicNotFoundException;

final class Stats
{
    public function __construct(private readonly StorageDriver $driver) {}

    /**
     * Records produced to the topic but not yet committed by the group.
     */
    public function lag(string $group, string $topic): int
    {
        return array_sum(array_column($this->partitions($group, $topic), 'lag'));
    }

    /**
     * @return list<array{partition: int, latest: ?string, committed: ?string, lag: int, owner: ?string, locked_until: ?CarbonImmutable}>
     */
    public function partitions(string $group, string $topic): array
    {
        $config = $this->driver->log()->topic($topic) ?? throw TopicNotFoundException::named($topic);
        $leases = $this->driver->leases()->leases($group, $topic);
        $now = CarbonImmutable::now();
        $rows = [];

        for ($p = 0; $p < $config->partitions; $p++) {
            $committed = $this->driver->offsets()->committed($group, $topic, $p);
            $lease = $leases[$p] ?? null;
            $active = $lease?->isActive($now) ?? false;

            $rows[] = [
                'partition' => $p,
                'latest' => $this->driver->log()->latestOffset($topic, $p),
                'committed' => $committed,
                'lag' => $this->driver->log()->countAfter($topic, $p, $committed),
                'owner' => $active ? $lease->memberId : null,
                'locked_until' => $active ? $lease->lockedUntil : null,
            ];
        }

        return $rows;
    }
}
