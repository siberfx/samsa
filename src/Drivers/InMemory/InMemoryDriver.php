<?php

namespace Siberfx\Samsa\Drivers\InMemory;

use Siberfx\Samsa\Contracts\DeadLetterStore;
use Siberfx\Samsa\Contracts\LeaseStore;
use Siberfx\Samsa\Contracts\LogStore;
use Siberfx\Samsa\Contracts\OffsetStore;
use Siberfx\Samsa\Contracts\Serializer;
use Siberfx\Samsa\Contracts\StorageDriver;

/**
 * Process-local driver for tests and local development. Implements the full
 * contract; state lives as long as the instance.
 */
final class InMemoryDriver implements StorageDriver
{
    private readonly InMemoryStore $store;

    public function __construct(?Serializer $serializer = null)
    {
        $this->store = new InMemoryStore($serializer);
    }

    public function log(): LogStore
    {
        return $this->store;
    }

    public function offsets(): OffsetStore
    {
        return $this->store;
    }

    public function leases(): LeaseStore
    {
        return $this->store;
    }

    public function deadLetters(): DeadLetterStore
    {
        return $this->store;
    }
}
