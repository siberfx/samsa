<?php

namespace Siberfx\Samsa;

use Closure;
use Illuminate\Contracts\Cache\Factory as CacheFactory;
use Illuminate\Contracts\Debug\ExceptionHandler;
use Illuminate\Contracts\Events\Dispatcher;
use Illuminate\Database\Connection;
use Illuminate\Database\DatabaseManager;
use Illuminate\Support\Manager;
use InvalidArgumentException;
use Siberfx\Samsa\Consumer\RetryPolicy;
use Siberfx\Samsa\Consumer\SubscriptionRegistry;
use Siberfx\Samsa\Consumer\Worker;
use Siberfx\Samsa\Contracts\BatchHandler;
use Siberfx\Samsa\Contracts\Handler;
use Siberfx\Samsa\Contracts\Partitioner;
use Siberfx\Samsa\Contracts\Serializer;
use Siberfx\Samsa\Contracts\StorageDriver;
use Siberfx\Samsa\Drivers\Database\DatabaseDriver;
use Siberfx\Samsa\Drivers\InMemory\InMemoryDriver;
use Siberfx\Samsa\Producer\Producer;
use Siberfx\Samsa\Producer\TopicRegistry;

/**
 * @mixin Client
 */
class SamsaManager extends Manager
{
    public function getDefaultDriver(): string
    {
        return (string) $this->config->get('samsa.default', 'database');
    }

    /**
     * Subscribe a handler to a topic under a consumer group.
     *
     * @param  class-string<Handler|BatchHandler>|Handler|BatchHandler|Closure  $handler
     */
    public function subscribe(string $group, string $topic, string|Handler|BatchHandler|Closure $handler): void
    {
        $this->subscriptions()->subscribe($group, $topic, $handler);
    }

    public function subscriptions(): SubscriptionRegistry
    {
        return $this->container->make(SubscriptionRegistry::class);
    }

    /**
     * A worker bound to the given driver (or the default one).
     */
    public function worker(?string $driver = null): Worker
    {
        $cache = $this->container->bound(CacheFactory::class) ? $this->container->make(CacheFactory::class)->store() : null;

        return new Worker(
            client: $this->driver($driver),
            subscriptions: $this->subscriptions(),
            container: $this->container,
            retry: RetryPolicy::fromConfig((array) $this->config->get('samsa.retry', [])),
            deadLetters: (bool) $this->config->get('samsa.dead_letter.enabled', true),
            events: $this->events(),
            exceptions: $this->container->bound(ExceptionHandler::class) ? $this->container->make(ExceptionHandler::class) : null,
            cache: $cache,
        );
    }

    protected function createDatabaseDriver(): Client
    {
        $config = (array) $this->config->get('samsa.drivers.database', []);
        $db = $this->container->make(DatabaseManager::class);

        /** @var Connection $connection */
        $connection = $db->connection($config['connection'] ?? null);

        return $this->makeClient('database', new DatabaseDriver($connection, $this->serializer(), (string) ($config['table_prefix'] ?? '')));
    }

    protected function createMemoryDriver(): Client
    {
        return $this->makeClient('memory', new InMemoryDriver($this->serializer()));
    }

    protected function createNatsDriver(): Client
    {
        throw new InvalidArgumentException('The NATS JetStream driver ships in Samsa v0.4.');
    }

    protected function createRedisDriver(): Client
    {
        throw new InvalidArgumentException('The Redis Streams driver ships in Samsa v0.5.');
    }

    /**
     * Wrap any StorageDriver (including your own) in a client.
     */
    public function makeClient(string $name, StorageDriver $driver): Client
    {
        $topics = new TopicRegistry(
            $driver->log(),
            (array) $this->config->get('samsa.defaults', []),
            (array) $this->config->get('samsa.topics', []),
            (bool) $this->config->get('samsa.auto_create_topics', false),
        );

        $producer = new Producer(
            $driver->log(),
            $topics,
            $this->container->make((string) $this->config->get('samsa.partitioner', Partitioner::class)),
            $this->events(),
            (string) $this->config->get('app.name', 'laravel'),
        );

        $db = $this->container->bound('db') ? $this->container->make(DatabaseManager::class) : null;

        return new Client($name, $driver, $producer, $topics, $db);
    }

    private function serializer(): Serializer
    {
        return $this->container->make((string) $this->config->get('samsa.serializer', Serializer::class));
    }

    private function events(): ?Dispatcher
    {
        return $this->container->bound(Dispatcher::class) ? $this->container->make(Dispatcher::class) : null;
    }
}
