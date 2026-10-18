<?php

namespace Siberfx\Samsa\Tests;

use Illuminate\Support\Sleep;
use Orchestra\Testbench\TestCase as Orchestra;
use Siberfx\Samsa\Facades\Samsa;
use Siberfx\Samsa\SamsaServiceProvider;

abstract class TestCase extends Orchestra
{
    protected function setUp(): void
    {
        parent::setUp();

        Sleep::fake();
    }

    protected function getPackageProviders($app): array
    {
        return [SamsaServiceProvider::class];
    }

    protected function getPackageAliases($app): array
    {
        return ['Samsa' => Samsa::class];
    }

    protected function defineEnvironment($app): void
    {
        // SQLite in memory by default; SAMSA_TEST_DB_CONNECTION=mysql|pgsql runs everything on a real server.
        $app['config']->set('database.default', env('SAMSA_TEST_DB_CONNECTION', 'testing'));
        $app['config']->set('samsa.default', 'database');
        $app['config']->set('samsa.retry.backoff_ms', [1, 1, 1]);
    }

    protected function defineDatabaseMigrations(): void
    {
        foreach (['create_samsa_tables', 'create_samsa_delayed_table'] as $stub) {
            $migration = require __DIR__."/../database/migrations/{$stub}.php.stub";
            $migration->down(); // leftovers from an interrupted run on a real server
            $migration->up();

            $this->beforeApplicationDestroyed(fn () => $migration->down());
        }
    }
}
