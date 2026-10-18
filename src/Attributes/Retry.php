<?php

namespace Siberfx\Samsa\Attributes;

use Attribute;

/**
 * Per-handler retry policy: #[Retry(maxAttempts: 5, backoffMs: [200, 1000, 5000])]
 */
#[Attribute(Attribute::TARGET_CLASS)]
final readonly class Retry
{
    /**
     * @param  list<int>|null  $backoffMs
     */
    public function __construct(
        public ?int $maxAttempts = null,
        public ?array $backoffMs = null,
    ) {}
}
