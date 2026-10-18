<?php

namespace Siberfx\Samsa\Testing;

use Illuminate\Database\DatabaseManager;
use Illuminate\Support\Collection;
use PHPUnit\Framework\Assert as PHPUnit;
use Siberfx\Samsa\AfterCommit;
use Siberfx\Samsa\Client;
use Siberfx\Samsa\Data\OutgoingRecord;
use Siberfx\Samsa\Data\RecordMetadata;
use Siberfx\Samsa\Producer\PendingRecord;
use Siberfx\Samsa\SamsaManager;

/**
 * Swapped in by Samsa::fake(): records what is produced to the faked topics
 * (all topics by default) instead of writing it. Everything else is forwarded
 * to the real manager or client.
 */
class SamsaFake
{
    /**
     * @param  list<string>|null  $topics  null = fake every topic
     */
    public function __construct(
        private readonly SamsaManager|Client $target,
        private readonly ?array $topics = null,
        private readonly ProducedRecords $produced = new ProducedRecords,
        private readonly ?DatabaseManager $db = null,
    ) {}

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
        if (! $this->fakes($topic)) {
            return $this->target->produce($topic, $payload, $key, $headers, $dedupKey, $eventType);
        }

        $record = new OutgoingRecord($key === null ? null : (string) $key, $payload, $headers, $dedupKey, $eventType);

        return $this->produced->add($topic, $record);
    }

    /**
     * @param  list<OutgoingRecord>  $records
     * @return list<RecordMetadata>
     */
    public function produceBatch(string $topic, array $records): array
    {
        if (! $this->fakes($topic)) {
            return $this->target->produceBatch($topic, $records);
        }

        return array_map(fn (OutgoingRecord $record) => $this->produced->add($topic, $record), $records);
    }

    public function topic(string $topic): PendingRecord
    {
        return new PendingRecord($this->produce(...), $topic);
    }

    public function afterCommit(): AfterCommit
    {
        return new AfterCommit($this->produce(...), $this->db);
    }

    public function driver(?string $driver = null): self
    {
        if ($driver === null || ! $this->target instanceof SamsaManager) {
            return $this;
        }

        return new self($this->target->driver($driver), $this->topics, $this->produced, $this->db);
    }

    // ------------------------------------------------------------ assertions

    /**
     * @return Collection<int, OutgoingRecord>
     */
    public function produced(string $topic, ?callable $callback = null): Collection
    {
        return collect($this->produced->for($topic))
            ->filter(fn (OutgoingRecord $record) => $callback === null || $callback($record))
            ->values();
    }

    /**
     * @param  (callable(OutgoingRecord): bool)|null  $callback
     */
    public function assertProduced(string $topic, ?callable $callback = null): void
    {
        PHPUnit::assertTrue(
            $this->produced($topic, $callback)->isNotEmpty(),
            "The expected record was not produced to [{$topic}].",
        );
    }

    /**
     * @param  (callable(OutgoingRecord): bool)|null  $callback
     */
    public function assertProducedTimes(string $topic, int $times, ?callable $callback = null): void
    {
        $count = $this->produced($topic, $callback)->count();

        PHPUnit::assertSame($times, $count, "The expected record was produced {$count} times to [{$topic}] instead of {$times} times.");
    }

    public function assertProducedCount(string $topic, int $count): void
    {
        $this->assertProducedTimes($topic, $count);
    }

    public function assertProducedWithKey(string $topic, string|int $key): void
    {
        $this->assertProduced($topic, fn (OutgoingRecord $record) => $record->key === (string) $key);
    }

    /**
     * @param  (callable(OutgoingRecord): bool)|null  $callback
     */
    public function assertNotProduced(string $topic, ?callable $callback = null): void
    {
        PHPUnit::assertTrue(
            $this->produced($topic, $callback)->isEmpty(),
            "An unexpected record was produced to [{$topic}].",
        );
    }

    public function assertNothingProduced(): void
    {
        $topics = $this->produced->topics();

        PHPUnit::assertSame([], $topics, 'Records were produced to: '.implode(', ', $topics).'.');
    }

    /**
     * @param  array<int, mixed>  $arguments
     */
    public function __call(string $method, array $arguments): mixed
    {
        return $this->target->{$method}(...$arguments);
    }

    private function fakes(string $topic): bool
    {
        return $this->topics === null || in_array($topic, $this->topics, true);
    }
}
