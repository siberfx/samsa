<?php

namespace Siberfx\Samsa\Events;

use Siberfx\Samsa\Data\OutgoingRecord;
use Siberfx\Samsa\Data\RecordMetadata;

final readonly class RecordProduced
{
    public function __construct(
        public RecordMetadata $metadata,
        public OutgoingRecord $record,
    ) {}
}
