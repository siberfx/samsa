<?php

namespace Siberfx\Samsa\Events;

final readonly class WorkerStopping
{
    public function __construct(
        public string $group,
        public string $memberId,
        public string $reason,
    ) {}
}
