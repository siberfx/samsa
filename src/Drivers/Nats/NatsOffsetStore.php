<?php

namespace Siberfx\Samsa\Drivers\Nats;

use Siberfx\Samsa\Contracts\OffsetStore;
use Siberfx\Samsa\Exceptions\LeaseLostException;

final class NatsOffsetStore implements OffsetStore
{
    public function __construct(
        private readonly NatsState $state,
        private readonly NatsLeaseStore $leases,
    ) {}

    public function committed(string $group, string $topic, int $partition): ?string
    {
        $offset = $this->state->get($this->key($group, $topic, $partition))['value']['offset'] ?? null;

        return $offset === null ? null : (string) $offset;
    }

    public function hasPosition(string $group, string $topic, int $partition): bool
    {
        return $this->state->get($this->key($group, $topic, $partition)) !== null;
    }

    public function commit(string $group, string $topic, int $partition, string $offset, ?string $memberId = null): void
    {
        // Fencing: refuse the commit unless $memberId still holds an unexpired lease.
        if ($memberId !== null && ! $this->leases->owns($group, $topic, $partition, $memberId)) {
            throw LeaseLostException::for($group, $topic, $partition, $memberId);
        }

        $this->state->put($this->key($group, $topic, $partition), ['offset' => $offset]);
    }

    public function seek(string $group, string $topic, int $partition, ?string $offset): void
    {
        $this->state->put($this->key($group, $topic, $partition), ['offset' => $offset]);
    }

    public function groups(string $topic): array
    {
        $groups = [];

        foreach ($this->state->keys('offsets.*.'.NatsConnection::token($topic).'.*') as $key) {
            $groups[NatsConnection::untoken(explode('.', $key)[1])] = true;
        }

        $groups = array_map('strval', array_keys($groups));
        sort($groups);

        return $groups;
    }

    public function allGroups(): array
    {
        $groups = [];

        foreach ($this->state->keys('offsets.>') as $key) {
            [, $group, $topic] = explode('.', $key);
            $groups[NatsConnection::untoken($group)][NatsConnection::untoken($topic)] = true;
        }

        ksort($groups);

        return array_map(function (array $topics) {
            $names = array_map('strval', array_keys($topics));
            sort($names);

            return $names;
        }, $groups);
    }

    private function key(string $group, string $topic, int $partition): string
    {
        return 'offsets.'.NatsConnection::token($group).'.'.NatsConnection::token($topic).'.'.$partition;
    }
}
