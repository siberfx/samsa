<?php

namespace Siberfx\Samsa\Consumer;

use Closure;
use Illuminate\Contracts\Cache\Repository as Cache;
use Illuminate\Contracts\Container\Container;
use Illuminate\Contracts\Debug\ExceptionHandler;
use Illuminate\Contracts\Events\Dispatcher;
use Illuminate\Support\Sleep;
use Illuminate\Support\Str;
use InvalidArgumentException;
use Siberfx\Samsa\Client;
use Siberfx\Samsa\Contracts\BatchHandler;
use Siberfx\Samsa\Contracts\Handler;
use Siberfx\Samsa\Data\Record;
use Siberfx\Samsa\Events\PartitionAssigned;
use Siberfx\Samsa\Events\PartitionRevoked;
use Siberfx\Samsa\Events\RecordConsumed;
use Siberfx\Samsa\Events\RecordDeadLettered;
use Siberfx\Samsa\Events\RecordFailed;
use Siberfx\Samsa\Events\WorkerStopping;
use Siberfx\Samsa\Exceptions\LeaseLostException;
use Throwable;

/**
 * The consume loop behind `samsa:consume`.
 *
 * Per loop: rebalance (heartbeat, renew, fair share) → read a batch from each
 * owned partition → handle each record, retrying in place → dead-letter what
 * still fails → commit (fenced by the lease) → sleep when idle.
 */
final class Worker
{
    public const RESTART_CACHE_KEY = 'samsa:restart';

    private const WAIT_SLICE_MS = 100;

    private string $memberId;

    private bool $shouldStop = false;

    private bool $stopNow = false;

    private string $stopReason = 'completed';

    /** Last full rebalance (heartbeat + renew + fair share), done between batches. */
    private float $lastRebalance = 0.0;

    /** Last lease renewal; renewals also happen mid-batch and during waits. */
    private float $lastRenewal = 0.0;

    private int $processed = 0;

    /** @var array<string, PartitionManager> */
    private array $managers = [];

    /** @var array<string, array<int, ?string>> topic => partition => position */
    private array $positions = [];

    /** @var (Closure(string, string): void)|null */
    private ?Closure $output = null;

    public function __construct(
        private readonly Client $client,
        private readonly SubscriptionRegistry $subscriptions,
        private readonly Container $container,
        private readonly RetryPolicy $retry,
        private readonly bool $deadLetters = true,
        private readonly ?Dispatcher $events = null,
        private readonly ?ExceptionHandler $exceptions = null,
        private readonly ?Cache $cache = null,
    ) {
        $this->memberId = (gethostname() ?: 'host').':'.getmypid().':'.Str::lower(Str::random(6));
    }

    /**
     * @param  Closure(string $level, string $message): void  $output
     */
    public function setOutput(Closure $output): self
    {
        $this->output = $output;

        return $this;
    }

    public function memberId(): string
    {
        return $this->memberId;
    }

    public function processed(): int
    {
        return $this->processed;
    }

    /**
     * Ask the worker to stop after the current record. With $now it also
     * abandons a pending retry backoff (the record is redelivered later).
     */
    public function stop(string $reason = 'stopped', bool $now = false): void
    {
        $this->shouldStop = true;
        $this->stopNow = $this->stopNow || $now;
        $this->stopReason = $reason;
    }

    /**
     * @param  list<string>  $topics
     */
    public function run(string $group, array $topics, WorkerOptions $options): int
    {
        $handlers = $this->resolveHandlers($group, $topics);
        $this->listenForSignals();

        $startedAt = microtime(true);
        $restartMarker = $this->cache?->get(self::RESTART_CACHE_KEY);

        foreach ($topics as $topic) {
            $this->managers[$topic] = new PartitionManager($this->client->storage(), $group, $topic, $this->memberId, $options);
        }

        $this->write('info', "Member {$this->memberId} joining group [{$group}] on [".implode(', ', $topics).'].');

        try {
            while (true) {
                if (microtime(true) - $this->lastRebalance >= $options->heartbeatSeconds) {
                    $this->rebalance($group, $options);
                }

                $didWork = false;

                foreach ($this->managers as $topic => $manager) {
                    foreach ($manager->owned() as $partition) {
                        if ($this->shouldStop) {
                            break 2;
                        }

                        $records = $this->client->storage()->log()->read($topic, $partition, $this->positions[$topic][$partition] ?? null, $options->batchSize);

                        if ($records === []) {
                            continue;
                        }

                        $didWork = true;
                        [$handler, $policy] = $handlers[$topic];
                        $this->processBatch($group, $topic, $partition, $records, $handler, $policy, $options);
                    }
                }

                if ($this->shouldStop || $this->limitReached($options, $startedAt, $restartMarker)) {
                    break;
                }

                if ($options->once) {
                    $this->stop('once');

                    break;
                }

                if (! $didWork) {
                    if ($options->stopWhenEmpty) {
                        $this->stop('empty');

                        break;
                    }

                    $this->wait($options->sleepMs, $group, $options, interruptible: true);
                }
            }
        } finally {
            foreach ($this->managers as $manager) {
                $manager->releaseAll();
            }

            $this->events?->dispatch(new WorkerStopping($group, $this->memberId, $this->stopReason));
            $this->write('info', "Member {$this->memberId} stopped ({$this->stopReason}) after {$this->processed} record(s).");
        }

        return 0;
    }

    /**
     * @param  list<Record>  $records
     */
    private function processBatch(string $group, string $topic, int $partition, array $records, object $handler, RetryPolicy $policy, WorkerOptions $options): void
    {
        $last = null;

        try {
            if ($handler instanceof BatchHandler) {
                $last = $this->processWithBatchHandler($group, $records, $handler, $policy, $options);
            } else {
                foreach ($records as $record) {
                    if ($this->shouldStop) {
                        break;
                    }

                    $this->processRecord($group, $record, $handler, $policy, $options);
                    $last = $record->offset;
                    $this->processed++;

                    if ($options->commit === 'record' && ! $this->commit($group, $topic, $partition, $last)) {
                        return;
                    }

                    if ($options->maxRecords !== null && $this->processed >= $options->maxRecords) {
                        $this->stop('max-records');

                        break;
                    }

                    if (! $this->renewIfDue($group, $options, $topic, $partition)) {
                        return; // lease lost: the new owner will redeliver from the last commit
                    }
                }
            }
        } catch (StopImmediately) {
            // Abandon the record being retried; commit what finished before it.
        }

        if ($last !== null && ($options->commit === 'batch' || $handler instanceof BatchHandler)) {
            $this->commit($group, $topic, $partition, $last);
        }
    }

    /**
     * @param  list<Record>  $records
     */
    private function processWithBatchHandler(string $group, array $records, BatchHandler $handler, RetryPolicy $policy, WorkerOptions $options): string
    {
        $error = $this->attempt($group, $records, fn (array $batch) => $handler->handleBatch($batch), $policy, $options);

        if ($error !== null) {
            // Isolate the failing records: run them one by one, each with its own retries.
            foreach ($records as $record) {
                $this->processRecord($group, $record, $handler, $policy, $options);
            }
        } else {
            foreach ($records as $record) {
                $this->events?->dispatch(new RecordConsumed($group, $record));
            }
        }

        $this->processed += count($records);

        return $records[array_key_last($records)]->offset;
    }

    private function processRecord(string $group, Record $record, object $handler, RetryPolicy $policy, WorkerOptions $options): void
    {
        $invoke = match (true) {
            $handler instanceof BatchHandler => fn (array $batch) => $handler->handleBatch($batch),
            $handler instanceof Closure => fn (array $batch) => $handler($batch[0]),
            default => fn (array $batch) => $handler->handle($batch[0]),
        };

        $error = $this->attempt($group, [$record], $invoke, $policy, $options);

        if ($error === null) {
            $this->events?->dispatch(new RecordConsumed($group, $record));

            return;
        }

        $original = $error->getPrevious() ?? $error;
        $this->exceptions?->report($original);

        if (! $this->deadLetters) {
            throw $original;
        }

        $failed = $this->client->storage()->deadLetters()->store($group, $record, $original, $error->attempts);
        $this->events?->dispatch(new RecordDeadLettered($failed));
        $this->write('warn', "Dead-lettered {$record->topic}/{$record->partition}@{$record->offset} after {$error->attempts} attempt(s): {$original->getMessage()}");
    }

    /**
     * Run $invoke with retries. Returns null on success, the last error otherwise.
     *
     * @param  list<Record>  $records
     * @param  Closure(list<Record>): void  $invoke
     */
    private function attempt(string $group, array $records, Closure $invoke, RetryPolicy $policy, WorkerOptions $options): ?AttemptsExhausted
    {
        for ($attempt = 1; ; $attempt++) {
            $batch = array_map(fn (Record $r) => $r->withDeliveryAttempt($attempt), $records);

            try {
                $invoke($batch);

                return null;
            } catch (Throwable $e) {
                $retry = $policy->shouldRetry($e, $attempt);

                foreach ($batch as $record) {
                    $this->events?->dispatch(new RecordFailed($group, $record, $e, $retry));
                }

                if (! $retry) {
                    return AttemptsExhausted::wrap($e, $attempt);
                }

                $this->wait($policy->backoffMs($attempt), $group, $options, interruptible: false);
            }
        }
    }

    private function commit(string $group, string $topic, int $partition, string $offset): bool
    {
        try {
            $this->client->storage()->offsets()->commit($group, $topic, $partition, $offset, $this->memberId);
            $this->positions[$topic][$partition] = $offset;

            return true;
        } catch (LeaseLostException) {
            $this->managers[$topic]->forget($partition);
            unset($this->positions[$topic][$partition]);
            $this->events?->dispatch(new PartitionRevoked($group, $topic, $partition, $this->memberId, lost: true));
            $this->write('warn', "Lost the lease on {$topic}/{$partition} before committing @{$offset}; it will be redelivered to the new owner.");

            return false;
        }
    }

    private function rebalance(string $group, WorkerOptions $options): void
    {
        foreach ($this->managers as $topic => $manager) {
            $changes = $manager->rebalance();

            foreach ($changes['lost'] as $partition) {
                unset($this->positions[$topic][$partition]);
                $this->events?->dispatch(new PartitionRevoked($group, $topic, $partition, $this->memberId, lost: true));
            }

            foreach ($changes['revoked'] as $partition) {
                unset($this->positions[$topic][$partition]);
                $this->events?->dispatch(new PartitionRevoked($group, $topic, $partition, $this->memberId));
            }

            foreach ($changes['assigned'] as $partition) {
                $this->events?->dispatch(new PartitionAssigned($group, $topic, $partition, $this->memberId));
            }

            if ($changes['assigned'] !== [] || $changes['revoked'] !== []) {
                $this->write('info', "[{$topic}] owning partitions: ".(implode(',', $manager->owned()) ?: 'none'));
            }

            // Reload positions every rebalance so `samsa:seek` takes effect on running workers.
            foreach ($manager->owned() as $partition) {
                $this->positions[$topic][$partition] = $this->position($group, $topic, $partition, $options);
            }
        }

        $this->lastRebalance = $this->lastRenewal = microtime(true);
    }

    /**
     * Renew leases mid-batch when due. Returns false when $partition was lost.
     */
    private function renewIfDue(string $group, WorkerOptions $options, ?string $topic = null, ?int $partition = null): bool
    {
        if (! $this->renewalDue($options)) {
            return true;
        }

        foreach ($this->managers as $name => $manager) {
            foreach ($manager->renew() as $lost) {
                unset($this->positions[$name][$lost]);
                $this->events?->dispatch(new PartitionRevoked($group, $name, $lost, $this->memberId, lost: true));
            }
        }

        $this->lastRenewal = microtime(true);

        return $topic === null || in_array($partition, $this->managers[$topic]->owned(), true);
    }

    private function position(string $group, string $topic, int $partition, WorkerOptions $options): ?string
    {
        $offsets = $this->client->storage()->offsets();

        if ($options->startFrom === 'latest' && ! $offsets->hasPosition($group, $topic, $partition)) {
            $latest = $this->client->storage()->log()->latestOffset($topic, $partition);
            $offsets->seek($group, $topic, $partition, $latest);

            return $latest;
        }

        return $offsets->committed($group, $topic, $partition);
    }

    private function renewalDue(WorkerOptions $options): bool
    {
        return microtime(true) - $this->lastRenewal >= $options->heartbeatSeconds;
    }

    /**
     * Sleep in small slices, renewing leases and reacting to stop requests.
     */
    private function wait(int $milliseconds, string $group, WorkerOptions $options, bool $interruptible): void
    {
        $remaining = $milliseconds;

        while ($remaining > 0) {
            if ($this->stopNow || ($interruptible && $this->shouldStop)) {
                if (! $interruptible) {
                    throw new StopImmediately;
                }

                return;
            }

            $slice = min(self::WAIT_SLICE_MS, $remaining);
            Sleep::for($slice)->milliseconds();
            $remaining -= $slice;

            $this->renewIfDue($group, $options);
        }
    }

    private function limitReached(WorkerOptions $options, float $startedAt, mixed $restartMarker): bool
    {
        $reason = match (true) {
            $options->maxTime !== null && microtime(true) - $startedAt >= $options->maxTime => 'max-time',
            memory_get_usage(true) / 1024 / 1024 >= $options->memoryMb => 'memory',
            $this->cache !== null && $this->cache->get(self::RESTART_CACHE_KEY) !== $restartMarker => 'restart',
            default => null,
        };

        if ($reason !== null) {
            $this->stop($reason);
        }

        return $reason !== null;
    }

    /**
     * @param  list<string>  $topics
     * @return array<string, array{0: object, 1: RetryPolicy}>
     */
    private function resolveHandlers(string $group, array $topics): array
    {
        $handlers = [];

        foreach ($topics as $topic) {
            $handler = $this->subscriptions->handler($group, $topic)
                ?? throw new InvalidArgumentException("No handler is subscribed for group [{$group}] on topic [{$topic}]. Add it to samsa.subscriptions or call Samsa::subscribe().");

            if (is_string($handler)) {
                $handler = $this->container->make($handler);
            }

            if (! $handler instanceof Handler && ! $handler instanceof BatchHandler && ! $handler instanceof Closure) {
                throw new InvalidArgumentException('Handlers must implement '.Handler::class.' or '.BatchHandler::class.', or be a closure.');
            }

            $handlers[$topic] = [$handler, $handler instanceof Closure ? $this->retry : $this->retry->forHandler($handler)];
        }

        return $handlers;
    }

    private function listenForSignals(): void
    {
        if (! extension_loaded('pcntl')) {
            return;
        }

        pcntl_async_signals(true);
        pcntl_signal(SIGTERM, fn () => $this->stop('signal'));
        pcntl_signal(SIGINT, fn () => $this->stop('signal'));
        pcntl_signal(SIGQUIT, fn () => $this->stop('signal', now: true));
    }

    private function write(string $level, string $message): void
    {
        if ($this->output !== null) {
            ($this->output)($level, $message);
        }
    }
}
