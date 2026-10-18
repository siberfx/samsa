<?php

namespace Siberfx\Samsa\Data;

/**
 * A record on its way into the log, before a partition and offset are assigned.
 */
final readonly class OutgoingRecord
{
    /**
     * @param  array<string, mixed>  $payload
     * @param  array<string, scalar|null>  $headers
     */
    public function __construct(
        public ?string $key,
        public array $payload,
        public array $headers = [],
        public ?string $dedupKey = null,
        public ?string $eventType = null,
    ) {}

    /**
     * @param  array<string, scalar|null>  $headers
     */
    public function withHeaders(array $headers): self
    {
        return new self($this->key, $this->payload, [...$this->headers, ...$headers], $this->dedupKey, $this->eventType);
    }
}
