<?php

namespace Siberfx\Samsa\Drivers\Database;

use Illuminate\Database\Connection;
use Siberfx\Samsa\Contracts\OffsetStore;
use Siberfx\Samsa\Exceptions\LeaseLostException;

final class DatabaseOffsetStore implements OffsetStore
{
    public function __construct(
        private readonly Connection $connection,
        private readonly Tables $tables,
        private readonly DatabaseClock $clock,
    ) {}

    public function committed(string $group, string $topic, int $partition): ?string
    {
        $offset = $this->connection->table($this->tables->offsets)
            ->where('group_name', $group)
            ->where('topic', $topic)
            ->where('partition', $partition)
            ->value('committed_offset');

        return $offset === null ? null : (string) $offset;
    }

    public function hasPosition(string $group, string $topic, int $partition): bool
    {
        return $this->connection->table($this->tables->offsets)
            ->where('group_name', $group)
            ->where('topic', $topic)
            ->where('partition', $partition)
            ->exists();
    }

    public function commit(string $group, string $topic, int $partition, string $offset, ?string $memberId = null): void
    {
        if ($memberId === null) {
            $this->write($group, $topic, $partition, $offset);

            return;
        }

        $this->connection->transaction(function () use ($group, $topic, $partition, $offset, $memberId) {
            // Fencing: lock the lease row and only commit while we still own it.
            $owned = $this->connection->table($this->tables->leases)
                ->where('group_name', $group)
                ->where('topic', $topic)
                ->where('partition', $partition)
                ->where('member_id', $memberId)
                ->where('locked_until', '>', DatabaseClock::format($this->clock->now()))
                ->lockForUpdate()
                ->exists();

            if (! $owned) {
                throw LeaseLostException::for($group, $topic, $partition, $memberId);
            }

            $this->write($group, $topic, $partition, $offset);
        });
    }

    public function seek(string $group, string $topic, int $partition, ?string $offset): void
    {
        $this->write($group, $topic, $partition, $offset);
    }

    public function groups(string $topic): array
    {
        return $this->connection->table($this->tables->offsets)
            ->where('topic', $topic)
            ->distinct()
            ->orderBy('group_name')
            ->pluck('group_name')
            ->map(fn ($group) => (string) $group)
            ->all();
    }

    public function allGroups(): array
    {
        $groups = [];

        $rows = $this->connection->table($this->tables->offsets)
            ->select(['group_name', 'topic'])
            ->distinct()
            ->orderBy('group_name')
            ->orderBy('topic')
            ->get();

        foreach ($rows as $row) {
            $groups[(string) $row->group_name][] = (string) $row->topic;
        }

        return $groups;
    }

    private function write(string $group, string $topic, int $partition, ?string $offset): void
    {
        $this->connection->table($this->tables->offsets)->upsert(
            [[
                'group_name' => $group,
                'topic' => $topic,
                'partition' => $partition,
                'committed_offset' => $offset === null ? null : (int) $offset,
                'updated_at' => DatabaseClock::format($this->clock->now()),
            ]],
            ['group_name', 'topic', 'partition'],
            ['committed_offset', 'updated_at'],
        );
    }
}
