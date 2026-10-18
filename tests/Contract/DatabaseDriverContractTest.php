<?php

namespace Siberfx\Samsa\Tests\Contract;

use Siberfx\Samsa\Contracts\StorageDriver;
use Siberfx\Samsa\SamsaManager;
use Siberfx\Samsa\Testing\StorageDriverContract;
use Siberfx\Samsa\Tests\TestCase;

/**
 * Runs on SQLite by default; set SAMSA_TEST_DB_CONNECTION=mysql|pgsql to run on a real server.
 */
final class DatabaseDriverContractTest extends TestCase
{
    use StorageDriverContract;

    protected function driver(): StorageDriver
    {
        return $this->app->make(SamsaManager::class)->driver('database')->storage();
    }
}
