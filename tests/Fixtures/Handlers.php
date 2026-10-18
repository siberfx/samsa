<?php

namespace Siberfx\Samsa\Tests\Fixtures;

use RuntimeException;
use Siberfx\Samsa\Attributes\Retry;
use Siberfx\Samsa\Contracts\BatchHandler;
use Siberfx\Samsa\Contracts\Handler;
use Siberfx\Samsa\Data\Record;
use Siberfx\Samsa\Exceptions\UnrecoverableRecordException;

final class Journal
{
    /** @var list<Record> */
    public static array $records = [];

    /** @var list<list<Record>> */
    public static array $batches = [];

    public static int $attempts = 0;

    /** @var list<mixed> */
    public static array $jobs = [];

    public static function reset(): void
    {
        self::$records = [];
        self::$batches = [];
        self::$attempts = 0;
        self::$jobs = [];
    }

    /**
     * @return list<mixed>
     */
    public static function payloads(string $field = 'n'): array
    {
        return array_map(fn (Record $r) => $r->payload[$field] ?? null, self::$records);
    }
}

final class RecordingHandler implements Handler
{
    public function handle(Record $record): void
    {
        Journal::$records[] = $record;
    }
}

/**
 * Fails while payload "fail" is true; records successes.
 */
final class SometimesFailingHandler implements Handler
{
    public function handle(Record $record): void
    {
        Journal::$attempts++;

        if (($record->payload['fail_until_attempt'] ?? 0) >= $record->deliveryAttempt) {
            throw new RuntimeException("attempt {$record->deliveryAttempt} failed");
        }

        if ($record->payload['fail'] ?? false) {
            throw new RuntimeException("record {$record->payload['n']} always fails");
        }

        if ($record->payload['unrecoverable'] ?? false) {
            throw new UnrecoverableRecordException('bad payload');
        }

        Journal::$records[] = $record;
    }
}

#[Retry(maxAttempts: 5, backoffMs: [1])]
final class FiveAttemptsHandler implements Handler
{
    public function handle(Record $record): void
    {
        Journal::$attempts++;

        throw new RuntimeException('nope');
    }
}

final class RecordingBatchHandler implements BatchHandler
{
    public function handleBatch(array $records): void
    {
        foreach ($records as $record) {
            if ($record->payload['fail'] ?? false) {
                throw new RuntimeException("record {$record->payload['n']} fails");
            }
        }

        Journal::$batches[] = $records;

        foreach ($records as $record) {
            Journal::$records[] = $record;
        }
    }
}
