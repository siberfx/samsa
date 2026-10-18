<?php

namespace Siberfx\Samsa\Drivers\Database;

use Illuminate\Database\Connection;
use Illuminate\Database\Query\Builder;
use Siberfx\Samsa\Contracts\LeaseStore;
use Siberfx\Samsa\Data\LeaseInfo;

final class DatabaseLeaseStore implements LeaseStore
{
    public function __construct(
        private readonly Connection $connection,
        private readonly Tables $tables,
        private readonly DatabaseClock $clock,
    ) {}

    public function acquire(string $group, string $topic, int $partition, string $memberId, int $ttlSeconds): bool
    {
        $key = ['group_name' => $group, 'topic' => $topic, 'partition' => $partition];

        $this->connection->table($this->tables->leases)->insertOrIgnore([
            ...$key,
            'member_id' => null,
            'locked_until' => null,
        ]);

        $now = $this->clock->now();
        $until = DatabaseClock::format($now->addSeconds($ttlSeconds));

        // One atomic statement: take the lease if it is ours, free, or expired.
        $affected = $this->connection->table($this->tables->leases)
            ->where($key)
            ->where(function (Builder $q) use ($memberId, $now) {
                $q->where('member_id', $memberId)
                    ->orWhereNull('locked_until')
                    ->orWhere('locked_until', '<', DatabaseClock::format($now));
            })
            ->update([
                'member_id' => $memberId,
                'locked_until' => $until,
            ]);

        if ($affected === 1) {
            return true;
        }

        // MySQL reports 0 affected rows when the values didn't change (a renewal
        // within the same microsecond), so confirm ownership explicitly.
        return $this->connection->table($this->tables->leases)
            ->where($key)
            ->where('member_id', $memberId)
            ->where('locked_until', $until)
            ->exists();
    }

    public function release(string $group, string $topic, int $partition, string $memberId): void
    {
        $this->connection->table($this->tables->leases)
            ->where('group_name', $group)
            ->where('topic', $topic)
            ->where('partition', $partition)
            ->where('member_id', $memberId)
            ->update(['member_id' => null, 'locked_until' => null]);
    }

    public function leases(string $group, string $topic): array
    {
        $leases = [];

        $rows = $this->connection->table($this->tables->leases)
            ->where('group_name', $group)
            ->where('topic', $topic)
            ->orderBy('partition')
            ->get();

        foreach ($rows as $row) {
            $leases[(int) $row->partition] = new LeaseInfo(
                (int) $row->partition,
                $row->member_id,
                DatabaseClock::parse($row->locked_until),
            );
        }

        return $leases;
    }

    public function heartbeat(string $group, string $topic, string $memberId, int $ttlSeconds): void
    {
        $this->connection->table($this->tables->members)->upsert(
            [[
                'group_name' => $group,
                'topic' => $topic,
                'member_id' => $memberId,
                'expires_at' => DatabaseClock::format($this->clock->now()->addSeconds($ttlSeconds)),
            ]],
            ['group_name', 'topic', 'member_id'],
            ['expires_at'],
        );
    }

    public function members(string $group, string $topic): array
    {
        return $this->connection->table($this->tables->members)
            ->where('group_name', $group)
            ->where('topic', $topic)
            ->where('expires_at', '>', DatabaseClock::format($this->clock->now()))
            ->orderBy('member_id')
            ->pluck('member_id')
            ->map(fn ($member) => (string) $member)
            ->all();
    }

    public function leave(string $group, string $topic, string $memberId): void
    {
        $this->connection->table($this->tables->members)
            ->where('group_name', $group)
            ->where('topic', $topic)
            ->where('member_id', $memberId)
            ->delete();
    }
}
