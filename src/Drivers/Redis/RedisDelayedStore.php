<?php

namespace Siberfx\Samsa\Drivers\Redis;

use Carbon\CarbonImmutable;
use Closure;
use DateTimeInterface;
use Illuminate\Support\Str;
use Siberfx\Samsa\Contracts\DelayedStore;
use Siberfx\Samsa\Data\OutgoingRecord;

/**
 * Delayed records: a sorted set scored by due time, plus a hash with the data.
 * A record is removed only after it has been produced, so a crash in between
 * can produce it twice but never loses it.
 */
final class RedisDelayedStore implements DelayedStore
{
    public function __construct(private readonly RedisConnection $redis) {}

    public function schedule(string $topic, OutgoingRecord $record, DateTimeInterface $availableAt): string
    {
        $id = (string) Str::orderedUuid();

        $this->redis->raw('HSET', $this->redis->delayedDataKey(), $id, (string) json_encode([
            'topic' => $topic,
            'key' => $record->key,
            'payload' => $record->payload,
            'headers' => $record->headers,
            'dedup_key' => $record->dedupKey,
            'event_type' => $record->eventType,
        ]));
        $this->redis->raw('ZADD', $this->redis->delayedKey(), CarbonImmutable::instance($availableAt)->format('U.u'), $id);

        return $id;
    }

    public function release(Closure $produce, int $limit = 500): int
    {
        $due = (array) $this->redis->raw('ZRANGEBYSCORE', $this->redis->delayedKey(), '-inf', CarbonImmutable::now()->format('U.u'), 'LIMIT', '0', (string) $limit);
        $released = 0;

        foreach (array_map('strval', $due) as $id) {
            $value = $this->redis->raw('HGET', $this->redis->delayedDataKey(), $id);

            if ($value !== null && $value !== false) {
                $data = (array) json_decode((string) $value, true);

                $produce((string) $data['topic'], new OutgoingRecord(
                    key: $data['key'] ?? null,
                    payload: (array) $data['payload'],
                    headers: (array) ($data['headers'] ?? []),
                    dedupKey: $data['dedup_key'] ?? null,
                    eventType: $data['event_type'] ?? null,
                ));
                $released++;
            }

            $this->redis->raw('ZREM', $this->redis->delayedKey(), $id);
            $this->redis->raw('HDEL', $this->redis->delayedDataKey(), $id);
        }

        return $released;
    }

    public function count(?string $topic = null): int
    {
        if ($topic === null) {
            return (int) $this->redis->raw('ZCARD', $this->redis->delayedKey());
        }

        $flat = (array) $this->redis->raw('HGETALL', $this->redis->delayedDataKey());
        $count = 0;

        for ($i = 1; $i < count($flat); $i += 2) {
            $count += ((array) json_decode((string) $flat[$i], true))['topic'] === $topic ? 1 : 0;
        }

        return $count;
    }
}
