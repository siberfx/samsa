<?php

namespace Siberfx\Samsa\Facades;

use Illuminate\Support\Facades\Facade;
use Siberfx\Samsa\Client;
use Siberfx\Samsa\SamsaManager;

/**
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
    protected static function getFacadeAccessor(): string
    {
        return 'samsa';
    }
}
