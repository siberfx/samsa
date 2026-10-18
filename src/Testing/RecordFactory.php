<?php

namespace Siberfx\Samsa\Testing;

use Carbon\CarbonImmutable;
use Illuminate\Support\Str;
use Siberfx\Samsa\Data\Record;

/**
 * Build records for handler unit tests.
 */
final class RecordFactory
{
    private static int $offset = 0;

    /**
     * @param  array<string, mixed>  $payload
     * @param  array<string, scalar|null>  $headers
     */
    public static function make(
        string $topic,
        array $payload = [],
        ?string $key = null,
        int $partition = 0,
        ?string $offset = null,
        array $headers = [],
        ?string $eventType = null,
        int $deliveryAttempt = 1,
        ?CarbonImmutable $createdAt = null,
    ): Record {
        $createdAt ??= CarbonImmutable::now('UTC');

        return new Record(
            topic: $topic,
            partition: $partition,
            offset: $offset ?? (string) self::$offset++,
            key: $key,
            payload: $payload,
            headers: [
                'message_id' => (string) Str::ulid(),
                'produced_at' => $createdAt->format('Y-m-d\TH:i:s.uP'),
                'producer' => 'tests',
                ...$headers,
            ],
            createdAt: $createdAt,
            eventType: $eventType,
            deliveryAttempt: $deliveryAttempt,
        );
    }
}
