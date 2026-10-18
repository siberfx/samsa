<?php

namespace Siberfx\Samsa\Events;

use Siberfx\Samsa\Data\Record;
use Throwable;

/**
 * Fired on every failed attempt.
 */
final readonly class RecordFailed
{
    public function __construct(
        public string $group,
        public Record $record,
        public Throwable $exception,
        public bool $willRetry,
    ) {}
}
