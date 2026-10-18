<?php

namespace Siberfx\Samsa\Tests\Feature;

use Siberfx\Samsa\Contracts\StorageDriver;
use Siberfx\Samsa\Drivers\Redis\RedisDriver;

/**
 * Needs REDIS_HOST (Redis 6.2+).
 */
final class RedisEndToEndTest extends DriverEndToEndTestCase
{
    protected function driverName(): string
    {
        return 'redis';
    }

    protected function available(): bool
    {
        return (bool) getenv('REDIS_HOST');
    }

    protected function defineEnvironment($app): void
    {
        parent::defineEnvironment($app);

        $app['config']->set('database.redis.client', getenv('SAMSA_TEST_REDIS_CLIENT') ?: (extension_loaded('redis') ? 'phpredis' : 'predis'));
    }

    protected function driverConfig(): array
    {
        return [
            'connection' => 'default',
            'prefix' => 'samsae2e'.bin2hex(random_bytes(3)).':',
        ];
    }

    protected function cleanUp(StorageDriver $storage): void
    {
        if ($storage instanceof RedisDriver) {
            $storage->destroy();
        }
    }
}
