<?php

namespace Siberfx\Samsa\Consumer;

use Carbon\CarbonImmutable;
use Siberfx\Samsa\Contracts\StorageDriver;
use Siberfx\Samsa\Exceptions\TopicNotFoundException;

/**
 * Keeps one member's fair share of a topic's partitions for its group.
 */
final class PartitionManager
{
    /** @var array<int, true> */
    private array $owned = [];

    public function __construct(
        private readonly StorageDriver $driver,
        private readonly string $group,
        private readonly string $topic,
        private readonly string $memberId,
        private readonly WorkerOptions $options,
    ) {}

    /**
     * Heartbeat, renew owned leases, release surplus and claim free partitions.
     *
     * @return array{assigned: list<int>, revoked: list<int>, lost: list<int>}
     */
    public function rebalance(): array
    {
        $leases = $this->driver->leases();
        $leases->heartbeat($this->group, $this->topic, $this->memberId, $this->options->leaseSeconds);

        $config = $this->driver->log()->topic($this->topic) ?? throw TopicNotFoundException::named($this->topic);
        $members = $leases->members($this->group, $this->topic);
        $candidates = $this->options->partitions === null
            ? range(0, $config->partitions - 1)
            : array_values(array_filter($this->options->partitions, fn (int $p) => $p < $config->partitions));

        $target = $this->options->partitions === null
            ? (int) ceil($config->partitions / max(1, count($members)))
            : count($candidates);

        if ($this->options->maxPartitionsPerWorker !== null) {
            $target = min($target, $this->options->maxPartitionsPerWorker);
        }

        $lost = $this->renew();
        $revoked = [];

        while (count($this->owned) > $target) {
            $partition = max(array_keys($this->owned));
            $this->release($partition);
            $revoked[] = $partition;
        }

        $assigned = count($this->owned) < $target
            ? $this->claim($candidates, $target, $members)
            : [];

        return ['assigned' => $assigned, 'revoked' => $revoked, 'lost' => $lost];
    }

    /**
     * Renew every owned lease. Returns the partitions whose lease was lost.
     *
     * @return list<int>
     */
    public function renew(): array
    {
        $lost = [];

        foreach (array_keys($this->owned) as $partition) {
            if (! $this->driver->leases()->acquire($this->group, $this->topic, $partition, $this->memberId, $this->options->leaseSeconds)) {
                unset($this->owned[$partition]);
                $lost[] = $partition;
            }
        }

        return $lost;
    }

    /**
     * @return list<int>
     */
    public function owned(): array
    {
        $owned = array_keys($this->owned);
        sort($owned);

        return $owned;
    }

    public function forget(int $partition): void
    {
        unset($this->owned[$partition]);
    }

    public function release(int $partition): void
    {
        $this->driver->leases()->release($this->group, $this->topic, $partition, $this->memberId);
        unset($this->owned[$partition]);
    }

    public function releaseAll(): void
    {
        foreach (array_keys($this->owned) as $partition) {
            $this->release($partition);
        }

        $this->driver->leases()->leave($this->group, $this->topic, $this->memberId);
    }

    /**
     * @param  list<int>  $candidates
     * @param  list<string>  $members
     * @return list<int>
     */
    private function claim(array $candidates, int $target, array $members): array
    {
        $now = CarbonImmutable::now();
        $leases = $this->driver->leases()->leases($this->group, $this->topic);

        $free = array_values(array_filter(
            $candidates,
            fn (int $p) => ! isset($this->owned[$p]) && ! (isset($leases[$p]) && $leases[$p]->isActive($now)),
        ));

        // Start at a member-specific position so concurrent members don't all race for partition 0.
        $index = array_search($this->memberId, $members, true);

        if ($index !== false && $free !== []) {
            $shift = ((int) $index * (int) ceil(count($candidates) / max(1, count($members)))) % count($free);
            $free = [...array_slice($free, $shift), ...array_slice($free, 0, $shift)];
        }

        $assigned = [];

        foreach ($free as $partition) {
            if (count($this->owned) >= $target) {
                break;
            }

            if ($this->driver->leases()->acquire($this->group, $this->topic, $partition, $this->memberId, $this->options->leaseSeconds)) {
                $this->owned[$partition] = true;
                $assigned[] = $partition;
            }
        }

        sort($assigned);

        return $assigned;
    }
}
