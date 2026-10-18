<?php

namespace Siberfx\Samsa;

use Illuminate\Database\DatabaseManager;
use Siberfx\Samsa\Consumer\OffsetManager;
use Siberfx\Samsa\Consumer\Stats;
use Siberfx\Samsa\Contracts\DeadLetterStore;
use Siberfx\Samsa\Contracts\StorageDriver;
use Siberfx\Samsa\Data\OutgoingRecord;
use Siberfx\Samsa\Data\RecordMetadata;
use Siberfx\Samsa\Data\RetentionPolicy;
use Siberfx\Samsa\Data\TopicConfig;
use Siberfx\Samsa\Drivers\Database\DatabaseDriver;
use Siberfx\Samsa\Exceptions\CrossConnectionTransactionException;
use Siberfx\Samsa\Producer\PendingRecord;
use Siberfx\Samsa\Producer\Producer;
use Siberfx\Samsa\Producer\TopicRegistry;

/**
 * Samsa bound to one driver. Samsa::produce() etc. forward to the default client;
 * Samsa::driver('name') returns another one.
 */
class Client
{
    public function __construct(
        private readonly string $name,
        private readonly StorageDriver $driver,
        private readonly Producer $producer,
        private readonly TopicRegistry $topics,
        private readonly ?DatabaseManager $db = null,
    ) {}

    public function name(): string
    {
        return $this->name;
    }

    public function storage(): StorageDriver
    {
        return $this->driver;
    }

    /**
     * @param  array<string, mixed>  $payload
     * @param  array<string, scalar|null>  $headers
     */
    public function produce(
        string $topic,
        array $payload,
        string|int|null $key = null,
        array $headers = [],
        ?string $dedupKey = null,
        ?string $eventType = null,
    ): RecordMetadata {
        $this->guardCrossConnectionTransaction();

        return $this->producer->produce($topic, new OutgoingRecord(
            key: $key === null ? null : (string) $key,
            payload: $payload,
            headers: $headers,
            dedupKey: $dedupKey,
            eventType: $eventType,
        ));
    }

    /**
     * @param  list<OutgoingRecord>  $records
     * @return list<RecordMetadata>
     */
    public function produceBatch(string $topic, array $records): array
    {
        $this->guardCrossConnectionTransaction();

        return $this->producer->produceBatch($topic, $records);
    }

    public function topic(string $topic): PendingRecord
    {
        return new PendingRecord($this, $topic);
    }

    /**
     * Produce only once the surrounding database transaction commits.
     */
    public function afterCommit(): AfterCommit
    {
        return new AfterCommit($this, $this->db);
    }

    public function createTopic(string $name, ?int $partitions = null, ?RetentionPolicy $retention = null): TopicConfig
    {
        $config = $this->driver->log()->ensureTopic($this->topics->configured($name, $partitions, $retention));
        $this->topics->forget($name);

        return $config;
    }

    public function growTopic(string $name, int $partitions): TopicConfig
    {
        $config = $this->driver->log()->growPartitions($name, $partitions);
        $this->topics->forget($name);

        return $config;
    }

    public function deleteTopic(string $name): void
    {
        $this->driver->log()->deleteTopic($name);
        $this->topics->forget($name);
    }

    public function topics(): TopicRegistry
    {
        return $this->topics;
    }

    public function offsets(string $group, string $topic): OffsetManager
    {
        return new OffsetManager($this->driver, $group, $topic);
    }

    public function stats(): Stats
    {
        return new Stats($this->driver);
    }

    public function deadLetters(): DeadLetterStore
    {
        return $this->driver->deadLetters();
    }

    /**
     * Re-produce a dead letter to its original topic and remove it from the store.
     * The message_id header is kept so consumers can recognise the duplicate.
     */
    public function retryDeadLetter(string $id): ?RecordMetadata
    {
        $failed = $this->driver->deadLetters()->find($id);

        if ($failed === null) {
            return null;
        }

        $record = $failed->record;

        $metadata = $this->producer->produce($record->topic, new OutgoingRecord(
            key: $record->key,
            payload: $record->payload,
            headers: [...$record->headers, 'samsa-retry-of' => "{$record->topic}/{$record->partition}/{$record->offset}"],
            eventType: $record->eventType,
        ));

        $this->driver->deadLetters()->forget($id);

        return $metadata;
    }

    /**
     * With the database driver on a different connection than an open transaction,
     * the record would silently escape that transaction.
     */
    private function guardCrossConnectionTransaction(): void
    {
        if ($this->db === null || ! $this->driver instanceof DatabaseDriver) {
            return;
        }

        $default = $this->db->connection();
        $samsa = $this->driver->connection();

        if ($default->getName() !== $samsa->getName() && $default->transactionLevel() > 0 && $samsa->transactionLevel() === 0) {
            throw CrossConnectionTransactionException::make((string) $samsa->getName(), (string) $default->getName());
        }
    }
}
