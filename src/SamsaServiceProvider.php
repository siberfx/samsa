<?php

namespace Siberfx\Samsa;

use Illuminate\Console\Scheduling\Schedule;
use Illuminate\Support\ServiceProvider;
use Siberfx\Samsa\Consumer\SubscriptionRegistry;
use Siberfx\Samsa\Contracts\Partitioner;
use Siberfx\Samsa\Contracts\Serializer;
use Siberfx\Samsa\Idempotency\OnlyOnce;
use Siberfx\Samsa\Producer\DefaultPartitioner;
use Siberfx\Samsa\Serialization\JsonSerializer;

class SamsaServiceProvider extends ServiceProvider
{
    public function register(): void
    {
        $this->mergeConfigFrom(__DIR__.'/../config/samsa.php', 'samsa');

        $this->app->singleton('samsa', fn ($app) => new SamsaManager($app));
        $this->app->alias('samsa', SamsaManager::class);
        $this->app->bind(Client::class, fn ($app) => $app->make('samsa')->driver());

        $this->app->singleton(SubscriptionRegistry::class, fn ($app) => new SubscriptionRegistry(
            (array) $app['config']->get('samsa.subscriptions', []),
        ));

        $this->app->bindIf(Serializer::class, JsonSerializer::class);
        $this->app->bindIf(Partitioner::class, DefaultPartitioner::class);

        $this->app->bind(OnlyOnce::class, fn ($app) => new OnlyOnce(
            $app['db'],
            $app['config']->get('samsa.drivers.database.connection'),
            (string) $app['config']->get('samsa.drivers.database.table_prefix', ''),
        ));
    }

    public function boot(): void
    {
        if (! $this->app->runningInConsole()) {
            return;
        }

        $this->publishes([
            __DIR__.'/../config/samsa.php' => config_path('samsa.php'),
        ], 'samsa-config');

        $this->publishesMigrations([
            __DIR__.'/../database/migrations/create_samsa_tables.php.stub' => database_path('migrations/'.date('Y_m_d_His').'_create_samsa_tables.php'),
        ], 'samsa-migrations');

        $this->commands([
            Console\ConsumeCommand::class,
            Console\RestartCommand::class,
            Console\ProduceCommand::class,
            Console\TailCommand::class,
            Console\StatusCommand::class,
            Console\GroupsCommand::class,
            Console\SeekCommand::class,
            Console\PruneCommand::class,
            Console\TopicCreateCommand::class,
            Console\TopicListCommand::class,
            Console\TopicAlterCommand::class,
            Console\TopicDeleteCommand::class,
            Console\FailedListCommand::class,
            Console\FailedShowCommand::class,
            Console\FailedRetryCommand::class,
            Console\FailedForgetCommand::class,
            Console\FailedFlushCommand::class,
        ]);

        $this->callAfterResolving(Schedule::class, function (Schedule $schedule) {
            if ($this->app['config']->get('samsa.schedule.prune', true)) {
                $schedule->command('samsa:prune')->hourly()->withoutOverlapping()->runInBackground();
            }
        });
    }
}
