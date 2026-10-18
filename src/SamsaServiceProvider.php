<?php

namespace Siberfx\Samsa;

use Illuminate\Console\Scheduling\Schedule;
use Illuminate\Container\Container;
use Illuminate\Contracts\Events\Dispatcher;
use Illuminate\Queue\QueueManager;
use Illuminate\Support\ServiceProvider;
use Siberfx\MessageBus\Transport\TransportManager;
use Siberfx\Samsa\Bridge\EventPublisher;
use Siberfx\Samsa\Bridge\PayloadNormalizer;
use Siberfx\Samsa\Bus\SamsaTransport;
use Siberfx\Samsa\Consumer\SubscriptionRegistry;
use Siberfx\Samsa\Contracts\Partitioner;
use Siberfx\Samsa\Contracts\Serializer;
use Siberfx\Samsa\Idempotency\OnlyOnce;
use Siberfx\Samsa\Producer\DefaultPartitioner;
use Siberfx\Samsa\Queue\SamsaConnector;
use Siberfx\Samsa\Serialization\JsonSerializer;

class SamsaServiceProvider extends ServiceProvider
{
    public function register(): void
    {
        $this->mergeConfigFrom(__DIR__.'/../config/samsa.php', 'samsa');

        // The real manager lives under its class name; 'samsa' (the facade accessor)
        // is an alias, so Samsa::fake() can swap 'samsa' without touching commands.
        $this->app->singleton(SamsaManager::class, fn ($app) => new SamsaManager($app));
        $this->app->alias(SamsaManager::class, 'samsa');
        $this->app->bind(Client::class, fn ($app) => $app->make(SamsaManager::class)->driver());

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

        $this->app->singleton(EventPublisher::class, fn ($app) => new EventPublisher(
            $app,
            new PayloadNormalizer,
            (array) $app['config']->get('samsa.bridge', []),
        ));
    }

    public function boot(): void
    {
        $this->registerQueueDriver();
        $this->registerEventBridge();
        $this->registerMessageBusTransport();

        if (! $this->app->runningInConsole()) {
            return;
        }

        $this->publishes([
            __DIR__.'/../config/samsa.php' => config_path('samsa.php'),
        ], 'samsa-config');

        $this->publishesMigrations([
            __DIR__.'/../database/migrations/create_samsa_tables.php.stub' => database_path('migrations/'.date('Y_m_d_His').'_create_samsa_tables.php'),
            __DIR__.'/../database/migrations/create_samsa_delayed_table.php.stub' => database_path('migrations/'.date('Y_m_d_His', time() + 1).'_create_samsa_delayed_table.php'),
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
            Console\ReleaseDelayedCommand::class,
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
            $config = $this->app['config'];

            if ($config->get('samsa.schedule.prune', true)) {
                $schedule->command('samsa:prune')->hourly()->withoutOverlapping()->runInBackground();
            }

            if ($this->shouldReleaseDelayed()) {
                $schedule->command('samsa:release-delayed')->everySecond()->withoutOverlapping();
            }
        });
    }

    private function registerQueueDriver(): void
    {
        $this->callAfterResolving('queue', function (QueueManager $manager) {
            $container = $this->app instanceof Container ? $this->app : Container::getInstance();
            $manager->addConnector('samsa', fn () => new SamsaConnector($container));
        });
    }

    private function registerEventBridge(): void
    {
        if (! $this->app['config']->get('samsa.bridge.enabled', true)) {
            return;
        }

        $this->callAfterResolving(Dispatcher::class, function (Dispatcher $events) {
            $events->listen('*', fn (string $event, array $payload) => $this->app->make(EventPublisher::class)->handleDispatched($event, $payload));
        });
    }

    private function registerMessageBusTransport(): void
    {
        if (! class_exists(TransportManager::class)) {
            return;
        }

        $this->callAfterResolving(TransportManager::class, function (TransportManager $transports) {
            $transports->extend('samsa', fn (array $config, string $name, $app) => new SamsaTransport(
                $app,
                $transports->serializerFor($name),
                (string) ($config['topic'] ?? $name),
                (string) ($config['group'] ?? 'message-bus'),
                $config['samsa_driver'] ?? null,
                (int) ($config['batch_size'] ?? 10),
                (int) ($config['lease_seconds'] ?? 90),
            ));
        });
    }

    /**
     * samsa.schedule.release_delayed: true / false, or null to enable it
     * automatically when a queue connection uses the samsa driver.
     */
    private function shouldReleaseDelayed(): bool
    {
        $setting = $this->app['config']->get('samsa.schedule.release_delayed');

        if ($setting !== null) {
            return filter_var($setting, FILTER_VALIDATE_BOOLEAN);
        }

        foreach ((array) $this->app['config']->get('queue.connections', []) as $connection) {
            if (($connection['driver'] ?? null) === 'samsa') {
                return true;
            }
        }

        return false;
    }
}
