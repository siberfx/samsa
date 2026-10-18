<?php

namespace Siberfx\Samsa\Tests\Contract;

use Basis\Nats\Client;
use PHPUnit\Framework\TestCase;
use Siberfx\Samsa\Contracts\StorageDriver;
use Siberfx\Samsa\Drivers\Nats\NatsConnection;
use Siberfx\Samsa\Drivers\Nats\NatsDriver;
use Siberfx\Samsa\Serialization\JsonSerializer;
use Siberfx\Samsa\Testing\StorageDriverContract;

/**
 * Needs a NATS server with JetStream: NATS_HOST=127.0.0.1 vendor/bin/phpunit
 */
final class NatsDriverContractTest extends TestCase
{
    use StorageDriverContract;

    private ?NatsDriver $nats = null;

    protected function setUp(): void
    {
        if (! getenv('NATS_HOST') || ! class_exists(Client::class)) {
            $this->markTestSkipped('Set NATS_HOST to run the NATS JetStream contract tests.');
        }
    }

    protected function tearDown(): void
    {
        $this->nats?->destroy();
        $this->nats?->connection()->disconnect();
    }

    protected function driver(): StorageDriver
    {
        return $this->nats = new NatsDriver(new NatsConnection([
            'host' => (string) getenv('NATS_HOST'),
            'port' => (int) (getenv('NATS_PORT') ?: 4222),
            'prefix' => 'samsatest'.bin2hex(random_bytes(3)),
            'storage' => 'memory',
            'timeout' => 2.0,
        ]), new JsonSerializer);
    }
}
