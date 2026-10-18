<?php

namespace Siberfx\Samsa\Data;

use Carbon\CarbonImmutable;

/**
 * A record stored in the dead letter store.
 */
final readonly class FailedRecord
{
    public function __construct(
        public string $id,
        public string $group,
        public Record $record,
        public int $attempts,
        public string $exception,
        public CarbonImmutable $failedAt,
    ) {}
}
