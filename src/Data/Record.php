<?php

namespace Siberfx\Samsa\Data;

use Carbon\CarbonImmutable;

/**
 * A record read from the log.
 */
final readonly class Record
{
    /**
     * @param  array<string, mixed>  $payload
     * @param  array<string, scalar|null>  $headers
     */
    public function __construct(
        public string $topic,
        public int $partition,
        public string $offset,
        public ?string $key,
        public array $payload,
        public array $headers,
        public CarbonImmutable $createdAt,
        public ?string $eventType = null,
        public int $deliveryAttempt = 1,
    ) {}

    public function header(string $name, mixed $default = null): mixed
    {
        return $this->headers[$name] ?? $default;
    }

    public function messageId(): ?string
    {
        $id = $this->headers['message_id'] ?? null;

        return $id === null ? null : (string) $id;
    }

    public function isRedelivery(): bool
    {
        return $this->deliveryAttempt > 1;
    }

    public function withDeliveryAttempt(int $attempt): self
    {
        return new self(
            $this->topic,
            $this->partition,
            $this->offset,
            $this->key,
            $this->payload,
            $this->headers,
            $this->createdAt,
            $this->eventType,
            $attempt,
        );
    }
}
