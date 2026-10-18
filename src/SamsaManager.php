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
use PHPUnit\Framework\Assert as PHPUnit;
use Siberfx\Samsa\Bridge\EventPublisher;
use Siberfx\Samsa\Consumer\RetryPolicy;
use Siberfx\Samsa\Consumer\SubscriptionRegistry;
use Siberfx\Samsa\Consumer\Worker;
use Siberfx\Samsa\Consumer\WorkerOptions;
use Siberfx\Samsa\Contracts\BatchHandler;
use Siberfx\Samsa\Contracts\DelayedStore;
use Siberfx\Samsa\Contracts\Handler;
use Siberfx\Samsa\Contracts\Partitioner;
use Siberfx\Samsa\Contracts\Serializer;
use Siberfx\Samsa\Contracts\StorageDriver;
use Siberfx\Samsa\Data\FailedRecord;
use Siberfx\Samsa\Data\RecordMetadata;
use Siberfx\Samsa\Drivers\Database\DatabaseDelayedStore;
use Siberfx\Samsa\Drivers\Database\DatabaseDriver;
use Siberfx\Samsa\Drivers\Database\Tables;
use Siberfx\Samsa\Drivers\InMemory\InMemoryDelayedStore;
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

    /**
     * Produce an event object: to the topics of its #[PublishTo] attributes, or
     * to $topic when given. Returns one metadata per record produced immediately
     * (records deferred until the transaction commits are not included).
     *
     * @return list<RecordMetadata>
     */
    public function publish(object $event, ?string $topic = null, string|int|null $key = null): array
    {
        return $this->container->make(EventPublisher::class)->publish($event, $topic, $key);
    }

    /**
     * Run a worker for the group until every partition is caught up (tests and scripts).
     * Returns the number of records processed.
     *
     * @param  string|list<string>  $topics
     */
    public function consumeUntilEmpty(string $group, string|array $topics, ?string $driver = null): int
    {
        $worker = $this->worker($driver);

        $worker->run($group, (array) $topics, WorkerOptions::fromConfig((array) $this->config->get('samsa.consumer', []), [
            'stopWhenEmpty' => true,
        ]));

        return $worker->processed();
    }

    /**
     * Move a group back to the beginning of a topic, so everything still retained is delivered again.
     */
    public function redeliver(string $group, string $topic, ?string $driver = null): void
    {
        $this->driver($driver)->offsets($group, $topic)->seekToEarliest();
    }

    /**
     * Assert that the group dead-lettered at least one matching record of the topic.
     *
     * @param  (callable(FailedRecord): bool)|null  $callback
     */
    public function assertDeadLettered(string $group, string $topic, ?callable $callback = null, ?string $driver = null): void
    {
        $matching = array_filter(
            $this->driver($driver)->deadLetters()->all($group, $topic, PHP_INT_MAX),
            fn (FailedRecord $failed) => $callback === null || $callback($failed),
        );

        PHPUnit::assertNotEmpty($matching, "No matching record of [{$topic}] was dead-lettered by group [{$group}].");
    }

    protected function createDatabaseDriver(): Client
    {
        $config = (array) $this->config->get('samsa.drivers.database', []);
        $db = $this->container->make(DatabaseManager::class);

        /** @var Connection $connection */
        $connection = $db->connection($config['connection'] ?? null);

        $prefix = (string) ($config['table_prefix'] ?? '');

        return $this->makeClient(
            'database',
            new DatabaseDriver($connection, $this->serializer(), $prefix),
            new DatabaseDelayedStore($connection, new Tables($prefix), $this->serializer()),
        );
    }

    protected function createMemoryDriver(): Client
    {
        return $this->makeClient('memory', new InMemoryDriver($this->serializer()), new InMemoryDelayedStore);
    }

    protected function createNatsDriver(): Client
    {
        throw new InvalidArgumentException('The NATS JetStream driver is planned for Samsa v1.2.');
    }

    protected function createRedisDriver(): Client
    {
        throw new InvalidArgumentException('The Redis Streams driver is planned for Samsa v1.3.');
    }

    /**
     * Wrap any StorageDriver (including your own) in a client.
     */
    public function makeClient(string $name, StorageDriver $driver, ?DelayedStore $delayed = null): Client
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

        return new Client($name, $driver, $producer, $topics, $db, $delayed);
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
