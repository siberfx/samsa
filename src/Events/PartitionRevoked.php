<?php

namespace Siberfx\Samsa\Events;

final readonly class PartitionRevoked
{
    public function __construct(
        public string $group,
        public string $topic,
        public int $partition,
        public string $memberId,
        public bool $lost = false,
    ) {}
}
