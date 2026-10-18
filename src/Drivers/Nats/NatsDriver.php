<?php

namespace Siberfx\Samsa\Drivers\Nats;

use Siberfx\Samsa\Contracts\DeadLetterStore;
use Siberfx\Samsa\Contracts\LeaseStore;
use Siberfx\Samsa\Contracts\LogStore;
use Siberfx\Samsa\Contracts\OffsetStore;
use Siberfx\Samsa\Contracts\Serializer;
use Siberfx\Samsa\Contracts\StorageDriver;

final class NatsDriver implements StorageDriver
{
    private readonly NatsState $state;

    private readonly NatsLogStore $log;

    private readonly NatsLeaseStore $leases;

    private readonly NatsOffsetStore $offsets;

    private readonly NatsDeadLetterStore $deadLetters;

    private readonly NatsDelayedStore $delayed;

    public function __construct(private readonly NatsConnection $nats, Serializer $serializer)
    {
        $this->state = new NatsState($nats);
        $this->log = new NatsLogStore($nats, $this->state, $serializer);
        $this->leases = new NatsLeaseStore($this->state);
        $this->offsets = new NatsOffsetStore($this->state, $this->leases);
        $this->deadLetters = new NatsDeadLetterStore($this->state);
        $this->delayed = new NatsDelayedStore($this->state);
    }

    public function log(): LogStore
    {
        return $this->log;
    }

    public function offsets(): OffsetStore
    {
        return $this->offsets;
    }

    public function leases(): LeaseStore
    {
        return $this->leases;
    }

    public function deadLetters(): DeadLetterStore
    {
        return $this->deadLetters;
    }

    public function delayed(): NatsDelayedStore
    {
        return $this->delayed;
    }

    public function connection(): NatsConnection
    {
        return $this->nats;
    }

    /**
     * Delete every stream this driver's prefix owns (tests and teardown).
     */
    public function destroy(): void
    {
        $prefix = strtoupper($this->nats->prefix()).'_';
        $names = $this->nats->api('STREAM.NAMES')->streams ?? [];

        foreach ((array) $names as $name) {
            if (str_starts_with((string) $name, $prefix)) {
                $this->nats->api("STREAM.DELETE.{$name}", [], [NatsConnection::ERR_STREAM_NOT_FOUND]);
            }
        }
    }
}
