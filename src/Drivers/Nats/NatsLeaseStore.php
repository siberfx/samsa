<?php

namespace Siberfx\Samsa\Drivers\Nats;

use Carbon\CarbonImmutable;
use Siberfx\Samsa\Contracts\LeaseStore;
use Siberfx\Samsa\Data\LeaseInfo;

/**
 * Leases are state entries updated with compare-and-swap on their revision, so
 * two members can never both win the same partition.
 */
final class NatsLeaseStore implements LeaseStore
{
    public function __construct(private readonly NatsState $state) {}

    public function acquire(string $group, string $topic, int $partition, string $memberId, int $ttlSeconds): bool
    {
        $key = $this->key($group, $topic, $partition);
        $now = $this->now();
        $lease = ['member' => $memberId, 'until' => $now + $ttlSeconds];
        $entry = $this->state->get($key);

        if ($entry === null) {
            return $this->state->create($key, $lease);
        }

        $current = $entry['value'];
        $free = ($current['member'] ?? null) === null || (float) ($current['until'] ?? 0) <= $now;

        if (($current['member'] ?? null) !== $memberId && ! $free) {
            return false;
        }

        return $this->state->update($key, $lease, $entry['revision']);
    }

    public function release(string $group, string $topic, int $partition, string $memberId): void
    {
        $key = $this->key($group, $topic, $partition);
        $entry = $this->state->get($key);

        if ($entry !== null && ($entry['value']['member'] ?? null) === $memberId) {
            $this->state->update($key, ['member' => null, 'until' => null], $entry['revision']);
        }
    }

    public function owns(string $group, string $topic, int $partition, string $memberId): bool
    {
        $lease = $this->state->get($this->key($group, $topic, $partition))['value'] ?? null;

        return $lease !== null && ($lease['member'] ?? null) === $memberId && (float) ($lease['until'] ?? 0) > $this->now();
    }

    public function leases(string $group, string $topic): array
    {
        $leases = [];
        $prefix = 'leases.'.NatsConnection::token($group).'.'.NatsConnection::token($topic).'.';

        foreach ($this->state->keys($prefix.'*') as $key) {
            $partition = (int) substr($key, strlen($prefix));
            $value = $this->state->get($key)['value'] ?? [];
            $until = $value['until'] ?? null;

            $leases[$partition] = new LeaseInfo(
                $partition,
                $value['member'] ?? null,
                $until === null ? null : CarbonImmutable::createFromTimestampUTC((float) $until),
            );
        }

        ksort($leases);

        return $leases;
    }

    public function heartbeat(string $group, string $topic, string $memberId, int $ttlSeconds): void
    {
        $this->state->put($this->memberKey($group, $topic, $memberId), ['until' => $this->now() + $ttlSeconds]);
    }

    public function members(string $group, string $topic): array
    {
        $members = [];
        $prefix = 'members.'.NatsConnection::token($group).'.'.NatsConnection::token($topic).'.';
        $now = $this->now();

        foreach ($this->state->keys($prefix.'*') as $key) {
            if ((float) ($this->state->get($key)['value']['until'] ?? 0) > $now) {
                $members[] = NatsConnection::untoken(substr($key, strlen($prefix)));
            }
        }

        sort($members);

        return $members;
    }

    public function leave(string $group, string $topic, string $memberId): void
    {
        $this->state->delete($this->memberKey($group, $topic, $memberId));
    }

    private function key(string $group, string $topic, int $partition): string
    {
        return 'leases.'.NatsConnection::token($group).'.'.NatsConnection::token($topic).'.'.$partition;
    }

    private function memberKey(string $group, string $topic, string $memberId): string
    {
        return 'members.'.NatsConnection::token($group).'.'.NatsConnection::token($topic).'.'.NatsConnection::token($memberId);
    }

    /**
     * Carbon's clock, so tests can move time; workers should run NTP-synced clocks.
     */
    private function now(): float
    {
        return (float) CarbonImmutable::now()->format('U.u');
    }
}
