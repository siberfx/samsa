<?php

namespace Siberfx\Samsa\Facades;

use Illuminate\Support\Facades\Facade;
use Siberfx\Samsa\Client;
use Siberfx\Samsa\SamsaManager;
use Siberfx\Samsa\Testing\SamsaFake;

/**
 * @method static list<\Siberfx\Samsa\Data\RecordMetadata> publish(object $event, string|null $topic = null, string|int|null $key = null)
 * @method static string produceLater(string $topic, \Siberfx\Samsa\Data\OutgoingRecord $record, \DateTimeInterface|int $availableAt)
 * @method static int releaseDelayed(int $limit = 500)
 * @method static int consumeUntilEmpty(string $group, string|list<string> $topics, string|null $driver = null)
 * @method static void redeliver(string $group, string $topic, string|null $driver = null)
 * @method static void assertDeadLettered(string $group, string $topic, callable|null $callback = null, string|null $driver = null)
 * @method static void assertProduced(string $topic, callable|null $callback = null)
 * @method static void assertProducedTimes(string $topic, int $times, callable|null $callback = null)
 * @method static void assertProducedCount(string $topic, int $count)
 * @method static void assertProducedWithKey(string $topic, string|int $key)
 * @method static void assertNotProduced(string $topic, callable|null $callback = null)
 * @method static void assertNothingProduced()
 * @method static \Siberfx\Samsa\Client driver(string|null $driver = null)
 * @method static \Siberfx\Samsa\Data\RecordMetadata produce(string $topic, array<string, mixed> $payload, string|int|null $key = null, array<string, scalar|null> $headers = [], string|null $dedupKey = null, string|null $eventType = null)
 * @method static list<\Siberfx\Samsa\Data\RecordMetadata> produceBatch(string $topic, list<\Siberfx\Samsa\Data\OutgoingRecord> $records)
 * @method static \Siberfx\Samsa\Producer\PendingRecord topic(string $topic)
 * @method static \Siberfx\Samsa\AfterCommit afterCommit()
 * @method static \Siberfx\Samsa\Data\TopicConfig createTopic(string $name, int|null $partitions = null, \Siberfx\Samsa\Data\RetentionPolicy|null $retention = null)
 * @method static \Siberfx\Samsa\Consumer\OffsetManager offsets(string $group, string $topic)
 * @method static \Siberfx\Samsa\Consumer\Stats stats()
 * @method static \Siberfx\Samsa\Contracts\DeadLetterStore deadLetters()
 * @method static \Siberfx\Samsa\Data\RecordMetadata|null retryDeadLetter(string $id)
 * @method static \Siberfx\Samsa\Contracts\StorageDriver storage()
 * @method static void subscribe(string $group, string $topic, string|\Siberfx\Samsa\Contracts\Handler|\Siberfx\Samsa\Contracts\BatchHandler|\Closure $handler)
 * @method static \Siberfx\Samsa\Consumer\SubscriptionRegistry subscriptions()
 * @method static \Siberfx\Samsa\Consumer\Worker worker(string|null $driver = null)
 *
 * @see SamsaManager
 * @see Client
 */
class Samsa extends Facade
{
    /**
     * Record produced records instead of writing them.
     *
     * @param  list<string>|null  $topics  only fake these topics; others are produced for real
     */
    public static function fake(?array $topics = null): SamsaFake
    {
        $root = static::getFacadeRoot();
        $manager = $root instanceof SamsaFake ? static::$app->make(SamsaManager::class) : $root;

        static::swap($fake = new SamsaFake($manager, $topics, db: static::$app->bound('db') ? static::$app->make('db') : null));

        return $fake;
    }

    protected static function getFacadeAccessor(): string
    {
        return 'samsa';
    }
}
