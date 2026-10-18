<?php

namespace Siberfx\Samsa\Events;

use Siberfx\Samsa\Data\FailedRecord;

final readonly class RecordDeadLettered
{
    public function __construct(
        public FailedRecord $failed,
    ) {}
}
