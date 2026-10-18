<?php

namespace Siberfx\Samsa\Producer;

use Siberfx\Samsa\Client;
use Siberfx\Samsa\Data\RecordMetadata;

/**
 * Fluent builder: Samsa::topic('orders')->key($id)->withHeaders([...])->produce($payload).
 */
final class PendingRecord
{
    private ?string $key = null;

    /** @var array<string, scalar|null> */
    private array $headers = [];

    private ?string $dedupKey = null;

    private ?string $eventType = null;

    public function __construct(
        private readonly Client $client,
        private readonly string $topic,
    ) {}

    public function key(string|int|null $key): self
    {
        $this->key = $key === null ? null : (string) $key;

        return $this;
    }

    /**
     * @param  array<string, scalar|null>  $headers
     */
    public function withHeaders(array $headers): self
    {
        $this->headers = [...$this->headers, ...$headers];

        return $this;
    }

    public function dedupKey(?string $dedupKey): self
    {
        $this->dedupKey = $dedupKey;

        return $this;
    }

    public function eventType(?string $eventType): self
    {
        $this->eventType = $eventType;

        return $this;
    }

    /**
     * @param  array<string, mixed>  $payload
     */
    public function produce(array $payload): RecordMetadata
    {
        return $this->client->produce($this->topic, $payload, $this->key, $this->headers, $this->dedupKey, $this->eventType);
    }
}
