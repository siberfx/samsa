<?php

namespace Siberfx\Samsa\Tests\Contract;

use Siberfx\Samsa\Contracts\StorageDriver;
use Siberfx\Samsa\Drivers\Redis\RedisDriver;
use Siberfx\Samsa\SamsaManager;
use Siberfx\Samsa\Testing\StorageDriverContract;
use Siberfx\Samsa\Tests\TestCase;

/**
 * Needs a Redis 6.2+ server: REDIS_HOST=127.0.0.1 vendor/bin/phpunit
 */
final class RedisDriverContractTest extends TestCase
{
    use StorageDriverContract;

    protected function setUp(): void
    {
        if (! getenv('REDIS_HOST')) {
            $this->markTestSkipped('Set REDIS_HOST to run the Redis Streams contract tests.');
        }

        parent::setUp();
    }

    protected function defineEnvironment($app): void
    {
        parent::defineEnvironment($app);

        $app['config']->set('database.redis.client', getenv('SAMSA_TEST_REDIS_CLIENT') ?: (extension_loaded('redis') ? 'phpredis' : 'predis'));
        $app['config']->set('samsa.drivers.redis', [
            'connection' => 'default',
            'prefix' => 'samsatest'.bin2hex(random_bytes(3)).':',
        ]);
    }

    protected function tearDown(): void
    {
        if ($this->app !== null) {
            $storage = $this->app->make(SamsaManager::class)->driver('redis')->storage();

            if ($storage instanceof RedisDriver) {
                $storage->destroy();
            }
        }

        parent::tearDown();
    }

    protected function driver(): StorageDriver
    {
        return $this->app->make(SamsaManager::class)->driver('redis')->storage();
    }
}
