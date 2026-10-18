<?php

namespace Siberfx\Samsa\Tests\Contract;

use PHPUnit\Framework\TestCase;
use Siberfx\Samsa\Contracts\StorageDriver;
use Siberfx\Samsa\Drivers\InMemory\InMemoryDriver;
use Siberfx\Samsa\Testing\StorageDriverContract;

final class InMemoryDriverContractTest extends TestCase
{
    use StorageDriverContract;

    protected function driver(): StorageDriver
    {
        return new InMemoryDriver;
    }
}
