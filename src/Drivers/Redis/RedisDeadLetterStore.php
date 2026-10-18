<?php

namespace Siberfx\Samsa\Drivers\Redis;

use Carbon\CarbonImmutable;
use DateTimeInterface;
use Illuminate\Support\Str;
use Siberfx\Samsa\Contracts\DeadLetterStore;
use Siberfx\Samsa\Data\FailedRecord;
use Siberfx\Samsa\Data\Record;
use Throwable;

/**
 * Dead letters in one hash: field = id, value = JSON.
 */
final class RedisDeadLetterStore implements DeadLetterStore
{
    public function __construct(private readonly RedisConnection $redis) {}

    public function store(string $group, Record $record, Throwable $error, int $attempts): FailedRecord
    {
        $failed = new FailedRecord(
            id: (string) Str::uuid(),
            group: $group,
            record: $record,
            attempts: $attempts,
            exception: (string) $error,
            failedAt: CarbonImmutable::now('UTC'),
        );

        $this->redis->raw('HSET', $this->redis->deadLettersKey(), $failed->id, (string) json_encode([
            'group' => $group,
            'topic' => $record->topic,
            'partition' => $record->partition,
            'offset' => $record->offset,
            'key' => $record->key,
            'event_type' => $record->eventType,
            'payload' => $record->payload,
            'headers' => $record->headers,
            'created_at' => $record->createdAt->format('Y-m-d\TH:i:s.uP'),
            'attempts' => $attempts,
            'exception' => $failed->exception,
            'failed_at' => $failed->failedAt->format('Y-m-d\TH:i:s.uP'),
        ]));

        return $failed;
    }

    public function find(string $id): ?FailedRecord
    {
        $value = $this->redis->raw('HGET', $this->redis->deadLettersKey(), $id);

        return $value === null || $value === false ? null : $this->hydrate($id, (array) json_decode((string) $value, true));
    }

    public function all(?string $group = null, ?string $topic = null, int $limit = 100): array
    {
        $flat = (array) $this->redis->raw('HGETALL', $this->redis->deadLettersKey());
        $failed = [];

        for ($i = 0; $i + 1 < count($flat); $i += 2) {
            $value = (array) json_decode((string) $flat[$i + 1], true);

            if (($group === null || $value['group'] === $group) && ($topic === null || $value['topic'] === $topic)) {
                $failed[] = $this->hydrate((string) $flat[$i], $value);
            }
        }

        usort($failed, fn (FailedRecord $a, FailedRecord $b) => $a->failedAt <=> $b->failedAt);

        return array_slice($failed, 0, $limit);
    }

    public function forget(string $id): bool
    {
        return (int) $this->redis->raw('HDEL', $this->redis->deadLettersKey(), $id) === 1;
    }

    public function flush(?DateTimeInterface $before = null): int
    {
        $count = 0;

        foreach ($this->all(limit: PHP_INT_MAX) as $failed) {
            if ($before === null || $failed->failedAt->lessThan($before)) {
                $count += (int) $this->redis->raw('HDEL', $this->redis->deadLettersKey(), $failed->id);
            }
        }

        return $count;
    }

    /**
     * @param  array<string, mixed>  $value
     */
    private function hydrate(string $id, array $value): FailedRecord
    {
        return new FailedRecord(
            id: $id,
            group: (string) $value['group'],
            record: new Record(
                topic: (string) $value['topic'],
                partition: (int) $value['partition'],
                offset: (string) $value['offset'],
                key: $value['key'] ?? null,
                payload: (array) $value['payload'],
                headers: (array) ($value['headers'] ?? []),
                createdAt: CarbonImmutable::parse((string) $value['created_at'])->utc(),
                eventType: $value['event_type'] ?? null,
            ),
            attempts: (int) $value['attempts'],
            exception: (string) $value['exception'],
            failedAt: CarbonImmutable::parse((string) $value['failed_at'])->utc(),
        );
    }
}
