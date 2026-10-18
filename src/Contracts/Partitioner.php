<?php

namespace Siberfx\Samsa\Contracts;

interface Partitioner
{
    public function partition(?string $key, int $partitionCount): int;
}
