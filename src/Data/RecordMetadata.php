<?php

namespace Siberfx\Samsa\Data;

final readonly class RecordMetadata
{
    public function __construct(
        public string $topic,
        public int $partition,
        public string $offset,
    ) {}
}
