<?php

namespace Siberfx\Samsa\Producer;

use Siberfx\Samsa\Contracts\Partitioner;

final class DefaultPartitioner implements Partitioner
{
    private int $roundRobin = 0;

    public function partition(?string $key, int $partitionCount): int
    {
        if ($key === null) {
            return $this->roundRobin++ % $partitionCount;
        }

        // crc32() is always non-negative on 64-bit PHP.
        return crc32($key) % $partitionCount;
    }
}
