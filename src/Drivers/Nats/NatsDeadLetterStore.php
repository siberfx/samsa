<?php

namespace Siberfx\Samsa\Drivers\Nats;

use Carbon\CarbonImmutable;
use DateTimeInterface;
use Illuminate\Support\Str;
use Siberfx\Samsa\Contracts\DeadLetterStore;
use Siberfx\Samsa\Data\FailedRecord;
use Siberfx\Samsa\Data\Record;
use Throwable;

final class NatsDeadLetterStore implements DeadLetterStore
{
    public function __construct(private readonly NatsState $state) {}

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

        $this->state->put($this->key($failed->id), [
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
        ]);

        return $failed;
    }

    public function find(string $id): ?FailedRecord
    {
        if (! Str::isUuid($id)) {
            return null;
        }

        $value = $this->state->get($this->key($id))['value'] ?? null;

        return $value === null ? null : $this->hydrate($id, $value);
    }

    public function all(?string $group = null, ?string $topic = null, int $limit = 100): array
    {
        $failed = [];

        foreach ($this->state->keys('dlq.*') as $key) {
            $value = $this->state->get($key)['value'] ?? null;

            if ($value === null
                || ($group !== null && $value['group'] !== $group)
                || ($topic !== null && $value['topic'] !== $topic)) {
                continue;
            }

            $failed[] = $this->hydrate($this->idFromKey($key), $value);
        }

        usort($failed, fn (FailedRecord $a, FailedRecord $b) => $a->failedAt <=> $b->failedAt);

        return array_slice($failed, 0, $limit);
    }

    public function forget(string $id): bool
    {
        if ($this->find($id) === null) {
            return false;
        }

        $this->state->delete($this->key($id));

        return true;
    }

    public function flush(?DateTimeInterface $before = null): int
    {
        $count = 0;

        foreach ($this->all(limit: PHP_INT_MAX) as $failed) {
            if ($before === null || $failed->failedAt->lessThan($before)) {
                $this->state->delete($this->key($failed->id));
                $count++;
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

    private function key(string $id): string
    {
        return 'dlq.'.str_replace('-', '', $id);
    }

    private function idFromKey(string $key): string
    {
        $hex = substr($key, strlen('dlq.'));

        return substr($hex, 0, 8).'-'.substr($hex, 8, 4).'-'.substr($hex, 12, 4).'-'.substr($hex, 16, 4).'-'.substr($hex, 20);
    }
}
