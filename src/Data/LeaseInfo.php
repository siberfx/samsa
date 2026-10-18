<?php

namespace Siberfx\Samsa\Data;

use Carbon\CarbonImmutable;

final readonly class LeaseInfo
{
    public function __construct(
        public int $partition,
        public ?string $memberId,
        public ?CarbonImmutable $lockedUntil,
    ) {}

    public function isActive(CarbonImmutable $now): bool
    {
        return $this->memberId !== null && $this->lockedUntil !== null && $this->lockedUntil->greaterThan($now);
    }
}
