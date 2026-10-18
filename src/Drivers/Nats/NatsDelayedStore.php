<?php

namespace Siberfx\Samsa\Drivers\Nats;

use Carbon\CarbonImmutable;
use Closure;
use DateTimeInterface;
use Illuminate\Support\Str;
use Siberfx\Samsa\Contracts\DelayedStore;
use Siberfx\Samsa\Data\OutgoingRecord;

/**
 * Delayed records as state entries. Releasing scans the delayed keys, so it
 * suits moderate volumes of delayed jobs.
 */
final class NatsDelayedStore implements DelayedStore
{
    public function __construct(private readonly NatsState $state) {}

    public function schedule(string $topic, OutgoingRecord $record, DateTimeInterface $availableAt): string
    {
        $id = str_replace('-', '', (string) Str::orderedUuid());

        $this->state->put('delayed.'.$id, [
            'topic' => $topic,
            'key' => $record->key,
            'payload' => $record->payload,
            'headers' => $record->headers,
            'dedup_key' => $record->dedupKey,
            'event_type' => $record->eventType,
            'at' => (float) CarbonImmutable::instance($availableAt)->format('U.u'),
        ]);

        return $id;
    }

    public function release(Closure $produce, int $limit = 500): int
    {
        $now = (float) CarbonImmutable::now()->format('U.u');
        $due = [];

        foreach ($this->state->keys('delayed.*') as $key) {
            $value = $this->state->get($key)['value'] ?? null;

            if ($value !== null && (float) $value['at'] <= $now) {
                $due[$key] = $value;
            }
        }

        uasort($due, fn (array $a, array $b) => $a['at'] <=> $b['at']);
        $released = 0;

        foreach (array_slice($due, 0, $limit, true) as $key => $value) {
            $produce((string) $value['topic'], new OutgoingRecord(
                key: $value['key'] ?? null,
                payload: (array) $value['payload'],
                headers: (array) ($value['headers'] ?? []),
                dedupKey: $value['dedup_key'] ?? null,
                eventType: $value['event_type'] ?? null,
            ));

            $this->state->delete((string) $key);
            $released++;
        }

        return $released;
    }

    public function count(?string $topic = null): int
    {
        $count = 0;

        foreach ($this->state->keys('delayed.*') as $key) {
            if ($topic === null || ($this->state->get($key)['value']['topic'] ?? null) === $topic) {
                $count++;
            }
        }

        return $count;
    }
}
