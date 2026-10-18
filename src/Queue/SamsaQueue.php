<?php

namespace Siberfx\Samsa\Queue;

use Carbon\CarbonImmutable;
use Illuminate\Container\Container;
use Illuminate\Contracts\Queue\ClearableQueue;
use Illuminate\Contracts\Queue\Job as JobContract;
use Illuminate\Contracts\Queue\Queue as QueueContract;
use Illuminate\Queue\Queue;
use Illuminate\Support\Str;
use Siberfx\Samsa\Client;
use Siberfx\Samsa\Consumer\PartitionManager;
use Siberfx\Samsa\Consumer\WorkerOptions;
use Siberfx\Samsa\Data\OutgoingRecord;
use Siberfx\Samsa\Exceptions\LeaseLostException;
use Siberfx\Samsa\SamsaManager;

/**
 * Laravel queue connection backed by a Samsa topic. Each `queue:work` process
 * joins the connection's consumer group and gets a fair share of partitions;
 * jobs with the same samsaKey() run strictly in order, one at a time.
 */
class SamsaQueue extends Queue implements ClearableQueue, QueueContract
{
    public const EVENT_TYPE = 'laravel.job';

    private readonly string $memberId;

    /** @var array<string, PartitionManager> */
    private array $managers = [];

    /** @var array<string, float> */
    private array $lastRebalance = [];

    /** @var array<string, array<int, ?string>> */
    private array $positions = [];

    /**
     * @param  array{topic?: string, group?: string, samsa_driver?: ?string, lease_seconds?: int, after_commit?: ?bool}  $connectionConfig
     */
    public function __construct(Container $container, private readonly array $connectionConfig)
    {
        $this->container = $container;
        $this->dispatchAfterCommit = $connectionConfig['after_commit'] ?? null;
        $this->memberId = (gethostname() ?: 'host').':'.getmypid().':'.Str::lower(Str::random(6));
    }

    public function size($queue = null): int
    {
        return $this->client()->stats()->lag($this->group(), $this->topicFor($queue));
    }

    /**
     * Records produced but not yet committed by the connection's group.
     */
    public function pendingSize($queue = null): int
    {
        return $this->size($queue);
    }

    public function delayedSize($queue = null): int
    {
        return $this->client()->delayed()->count($this->topicFor($queue));
    }

    /**
     * A log has no reservations: a job being handled is simply not committed yet.
     */
    public function reservedSize($queue = null): int
    {
        return 0;
    }

    /**
     * Unix timestamp of the oldest record the group has not committed yet.
     */
    public function creationTimeOfOldestPendingJob($queue = null): ?int
    {
        $topic = $this->topicFor($queue);
        $storage = $this->client()->storage();
        $config = $storage->log()->topic($topic);
        $oldest = null;

        for ($p = 0; $config !== null && $p < $config->partitions; $p++) {
            $next = $storage->log()->read($topic, $p, $storage->offsets()->committed($this->group(), $topic, $p), 1)[0] ?? null;

            if ($next !== null && ($oldest === null || $next->createdAt->getTimestamp() < $oldest)) {
                $oldest = $next->createdAt->getTimestamp();
            }
        }

        return $oldest;
    }

    public function push($job, $data = '', $queue = null): mixed
    {
        return $this->enqueueUsing(
            $job,
            $this->createPayload($job, $this->getQueue($queue), $data),
            $queue,
            null,
            fn (string $payload, ?string $queue) => $this->pushRaw($payload, $queue, ['key' => $this->keyFor($job)]),
        );
    }

    /**
     * @param  array{key?: ?string, attempts?: int}  $options
     */
    public function pushRaw($payload, $queue = null, array $options = []): mixed
    {
        // Resolved per call so Samsa::fake() captures queued jobs too.
        $this->container->make('samsa')->driver($this->connectionConfig['samsa_driver'] ?? null)->produce(
            $this->topicFor($queue),
            ['job' => $payload, 'attempts' => (int) ($options['attempts'] ?? 0)],
            $options['key'] ?? null,
            eventType: self::EVENT_TYPE,
        );

        return json_decode($payload, true)['uuid'] ?? null;
    }

    public function later($delay, $job, $data = '', $queue = null): mixed
    {
        return $this->enqueueUsing(
            $job,
            $this->createPayload($job, $this->getQueue($queue), $data, $delay),
            $queue,
            $delay,
            fn (string $payload, ?string $queue, $delay) => $this->laterRaw($delay, $payload, $queue, ['key' => $this->keyFor($job)]),
        );
    }

    /**
     * @param  array{key?: ?string, attempts?: int}  $options
     */
    public function laterRaw(mixed $delay, string $payload, ?string $queue = null, array $options = []): mixed
    {
        $this->client()->produceLater(
            $this->topicFor($queue),
            new OutgoingRecord(
                key: $options['key'] ?? null,
                payload: ['job' => $payload, 'attempts' => (int) ($options['attempts'] ?? 0)],
                eventType: self::EVENT_TYPE,
            ),
            CarbonImmutable::createFromTimestamp($this->availableAt($delay)),
        );

        return json_decode($payload, true)['uuid'] ?? null;
    }

    public function pop($queue = null): ?JobContract
    {
        $topic = $this->topicFor($queue);
        $manager = $this->manager($topic);

        if (microtime(true) - ($this->lastRebalance[$topic] ?? 0.0) >= $this->heartbeatSeconds()) {
            $this->rebalance($topic, $manager);
        }

        $log = $this->client()->storage()->log();

        foreach ($manager->owned() as $partition) {
            $records = $log->read($topic, $partition, $this->positions[$topic][$partition] ?? null, 1);

            if ($records !== []) {
                return new SamsaJob($this->container, $this, $records[0], $this->connectionName, $this->getQueue($queue));
            }
        }

        return null;
    }

    public function clear($queue): int
    {
        $topic = $this->topicFor($queue);
        $cleared = $this->size($queue);
        $this->client()->offsets($this->group(), $topic)->seekToLatest();
        unset($this->positions[$topic]);

        return $cleared;
    }

    /**
     * @internal Called by SamsaJob::delete(): the job is done, move the group past it.
     */
    public function acknowledge(SamsaJob $job): void
    {
        $record = $job->record();

        try {
            $this->client()->storage()->offsets()->commit($this->group(), $record->topic, $record->partition, $record->offset, $this->memberId);
            $this->positions[$record->topic][$record->partition] = $record->offset;
        } catch (LeaseLostException) {
            // Another worker owns the partition now and will redeliver from the last commit.
            $this->manager($record->topic)->forget($record->partition);
            unset($this->positions[$record->topic][$record->partition]);
        }
    }

    /**
     * @internal Called by SamsaJob::release(): re-produce with one more attempt, then move past the original.
     */
    public function releaseJob(SamsaJob $job, int $delay): void
    {
        $record = $job->record();
        $options = ['key' => $record->key, 'attempts' => $job->attempts()];

        if ($delay > 0) {
            $this->laterRaw($delay, $job->getRawBody(), $job->getQueue(), $options);
        } else {
            $this->pushRaw($job->getRawBody(), $job->getQueue(), $options);
        }

        $this->acknowledge($job);
    }

    /**
     * Give up all partitions (on worker shutdown) so others take over immediately.
     */
    public function releaseLeases(): void
    {
        foreach ($this->managers as $manager) {
            $manager->releaseAll();
        }

        $this->managers = [];
        $this->positions = [];
        $this->lastRebalance = [];
    }

    public function __destruct()
    {
        try {
            $this->releaseLeases();
        } catch (\Throwable) {
            // The connection may already be gone during shutdown; leases expire on their own.
        }
    }

    public function topicFor(?string $queue): string
    {
        $queue = $this->getQueue($queue);

        return $queue === 'default' ? (string) ($this->connectionConfig['topic'] ?? 'laravel-jobs') : $queue;
    }

    public function getQueue(?string $queue): string
    {
        return $queue ?: 'default';
    }

    private function rebalance(string $topic, PartitionManager $manager): void
    {
        $changes = $manager->rebalance();

        foreach ([...$changes['revoked'], ...$changes['lost']] as $partition) {
            unset($this->positions[$topic][$partition]);
        }

        foreach ($manager->owned() as $partition) {
            $this->positions[$topic][$partition] = $this->client()->storage()->offsets()->committed($this->group(), $topic, $partition);
        }

        $this->lastRebalance[$topic] = microtime(true);
    }

    private function manager(string $topic): PartitionManager
    {
        return $this->managers[$topic] ??= new PartitionManager(
            $this->client()->storage(),
            $this->group(),
            $topic,
            $this->memberId,
            new WorkerOptions(leaseSeconds: $this->leaseSeconds(), heartbeatSeconds: $this->heartbeatSeconds()),
        );
    }

    private function keyFor(mixed $job): ?string
    {
        if (is_object($job) && method_exists($job, 'samsaKey')) {
            $key = $job->samsaKey();

            return $key === null ? null : (string) $key;
        }

        return null;
    }

    private function client(): Client
    {
        return $this->container->make(SamsaManager::class)->driver($this->connectionConfig['samsa_driver'] ?? null);
    }

    private function group(): string
    {
        return (string) ($this->connectionConfig['group'] ?? 'laravel-queue');
    }

    /**
     * A job must finish within the lease: queue:work can't renew it mid-job.
     */
    private function leaseSeconds(): int
    {
        return max(2, (int) ($this->connectionConfig['lease_seconds'] ?? 90));
    }

    private function heartbeatSeconds(): int
    {
        return max(1, intdiv($this->leaseSeconds(), 3));
    }
}
