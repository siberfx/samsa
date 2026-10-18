<?php

namespace Siberfx\Samsa\Drivers\Database;

use Carbon\CarbonImmutable;
use Closure;
use DateTimeInterface;
use Illuminate\Database\Connection;
use Siberfx\Samsa\Contracts\DelayedStore;
use Siberfx\Samsa\Contracts\Serializer;
use Siberfx\Samsa\Data\OutgoingRecord;

final class DatabaseDelayedStore implements DelayedStore
{
    public function __construct(
        private readonly Connection $connection,
        private readonly Tables $tables,
        private readonly Serializer $serializer,
    ) {}

    public function schedule(string $topic, OutgoingRecord $record, DateTimeInterface $availableAt): string
    {
        return (string) $this->connection->table($this->tables->delayed)->insertGetId([
            'topic' => $topic,
            'key' => $record->key,
            'dedup_key' => $record->dedupKey,
            'event_type' => $record->eventType,
            'payload' => $this->serializer->encode($record->payload),
            'headers' => $this->serializer->encode($record->headers),
            'available_at' => DatabaseClock::format(CarbonImmutable::instance($availableAt)),
        ]);
    }

    public function release(Closure $produce, int $limit = 500): int
    {
        // Producing and deleting in one transaction: on the same connection as the
        // log, a delayed record is moved exactly once even if this process dies.
        return $this->connection->transaction(function () use ($produce, $limit) {
            $rows = $this->connection->table($this->tables->delayed)
                ->where('available_at', '<=', DatabaseClock::format(CarbonImmutable::now('UTC')))
                ->orderBy('available_at')
                ->orderBy('id')
                ->limit($limit)
                ->lockForUpdate()
                ->get();

            foreach ($rows as $row) {
                $produce((string) $row->topic, new OutgoingRecord(
                    key: $row->key,
                    payload: $this->serializer->decode((string) $row->payload),
                    headers: $row->headers === null ? [] : $this->serializer->decode((string) $row->headers),
                    dedupKey: $row->dedup_key,
                    eventType: $row->event_type,
                ));
            }

            if ($rows->isNotEmpty()) {
                $this->connection->table($this->tables->delayed)->whereIn('id', $rows->pluck('id'))->delete();
            }

            return $rows->count();
        });
    }

    public function count(?string $topic = null): int
    {
        return $this->connection->table($this->tables->delayed)
            ->when($topic !== null, fn ($q) => $q->where('topic', $topic))
            ->count();
    }
}
