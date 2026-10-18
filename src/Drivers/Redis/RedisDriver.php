<?php

namespace Siberfx\Samsa\Drivers\Redis;

use Siberfx\Samsa\Contracts\DeadLetterStore;
use Siberfx\Samsa\Contracts\LeaseStore;
use Siberfx\Samsa\Contracts\LogStore;
use Siberfx\Samsa\Contracts\OffsetStore;
use Siberfx\Samsa\Contracts\Serializer;
use Siberfx\Samsa\Contracts\StorageDriver;

final class RedisDriver implements StorageDriver
{
    private readonly RedisLogStore $log;

    private readonly RedisLeaseStore $leases;

    private readonly RedisOffsetStore $offsets;

    private readonly RedisDeadLetterStore $deadLetters;

    private readonly RedisDelayedStore $delayed;

    public function __construct(private readonly RedisConnection $redis, Serializer $serializer)
    {
        $this->log = new RedisLogStore($redis, $serializer);
        $this->leases = new RedisLeaseStore($redis);
        $this->offsets = new RedisOffsetStore($redis, $this->leases);
        $this->deadLetters = new RedisDeadLetterStore($redis);
        $this->delayed = new RedisDelayedStore($redis);
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

    public function delayed(): RedisDelayedStore
    {
        return $this->delayed;
    }

    /**
     * Delete every key under this driver's prefix (tests and teardown).
     */
    public function destroy(): void
    {
        foreach (array_chunk($this->redis->scan('*'), 500) as $keys) {
            $this->redis->raw('DEL', ...$keys);
        }
    }
}
