<?php

namespace Siberfx\Samsa\Events;

use Siberfx\Samsa\Data\Record;

final readonly class RecordConsumed
{
    public function __construct(
        public string $group,
        public Record $record,
    ) {}
}
