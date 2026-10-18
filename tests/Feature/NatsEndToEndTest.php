<?php

namespace Siberfx\Samsa\Tests\Feature;

use Basis\Nats\Client;
use Siberfx\Samsa\Contracts\StorageDriver;
use Siberfx\Samsa\Drivers\Nats\NatsDriver;

/**
 * Needs NATS_HOST (e.g. docker run -p 4222:4222 nats:2.10-alpine --jetstream).
 */
final class NatsEndToEndTest extends DriverEndToEndTestCase
{
    protected function driverName(): string
    {
        return 'nats';
    }

    protected function available(): bool
    {
        return (bool) getenv('NATS_HOST') && class_exists(Client::class);
    }

    protected function driverConfig(): array
    {
        return [
            'host' => (string) getenv('NATS_HOST'),
            'port' => (int) (getenv('NATS_PORT') ?: 4222),
            'prefix' => 'samsae2e'.bin2hex(random_bytes(3)),
            'storage' => 'memory',
            'timeout' => 2.0,
        ];
    }

    protected function cleanUp(StorageDriver $storage): void
    {
        if ($storage instanceof NatsDriver) {
            $storage->destroy();
            $storage->connection()->disconnect();
        }
    }
}
