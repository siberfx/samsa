<?php

namespace Siberfx\Samsa\Bus;

use Carbon\CarbonImmutable;
use Illuminate\Contracts\Container\Container;
use Illuminate\Support\Str;
use Siberfx\MessageBus\Contracts\ListableReceiverInterface;
use Siberfx\MessageBus\Contracts\MessageCountAwareInterface;
use Siberfx\MessageBus\Contracts\SerializerInterface;
use Siberfx\MessageBus\Contracts\TransportInterface;
use Siberfx\MessageBus\Envelope;
use Siberfx\MessageBus\Exception\MessageDecodingFailedException;
use Siberfx\MessageBus\Stamp\DelayStamp;
use Siberfx\MessageBus\Stamp\TransportMessageIdStamp;
use Siberfx\Samsa\Client;
use Siberfx\Samsa\Consumer\PartitionManager;
use Siberfx\Samsa\Consumer\WorkerOptions;
use Siberfx\Samsa\Data\OutgoingRecord;
use Siberfx\Samsa\Data\Record;
use Siberfx\Samsa\Exceptions\LeaseLostException;
use Siberfx\Samsa\SamsaManager;

/**
 * A siberfx/message-bus transport backed by a Samsa topic and consumer group.
 *
 * send()   → produce (or delay, for DelayStamp) with the message's partition key
 * get()    → read the next batch from the partitions this worker leases
 * ack()    → mark done; the offset is committed once every earlier record is done
 * reject() → mark done too: message-bus has already re-sent it or moved it to the failure transport
 */
class SamsaTransport implements ListableReceiverInterface, MessageCountAwareInterface, TransportInterface
{
    public const EVENT_TYPE_HEADER = 'type';

    private readonly string $memberId;

    private ?PartitionManager $partitions = null;

    private float $lastRebalance = 0.0;

    /** @var array<int, ?string> partition => last offset handed out */
    private array $positions = [];

    /** @var array<int, array<string, bool>> partition => offset => done, in read order */
    private array $inFlight = [];

    public function __construct(
        private readonly Container $container,
        private readonly SerializerInterface $serializer,
        private readonly string $topic,
        private readonly string $group = 'message-bus',
        private readonly ?string $driver = null,
        private readonly int $batchSize = 10,
        private readonly int $leaseSeconds = 90,
    ) {
        $this->memberId = (gethostname() ?: 'host').':'.getmypid().':'.Str::lower(Str::random(6));
    }

    public function send(Envelope $envelope): Envelope
    {
        $encoded = $this->serializer->encode($envelope);
        $payload = ['body' => $encoded['body'], 'headers' => $encoded['headers'] ?? []];
        $key = $this->partitionKey($envelope);
        $type = (string) ($payload['headers'][self::EVENT_TYPE_HEADER] ?? $envelope->getMessage()::class);
        $delay = $envelope->last(DelayStamp::class)->delay ?? 0;

        if ($delay > 0) {
            $id = $this->client()->produceLater(
                $this->topic,
                new OutgoingRecord($key, $payload, eventType: $type),
                CarbonImmutable::now('UTC')->addMilliseconds($delay),
            );

            return $envelope->with(new TransportMessageIdStamp("delayed:{$id}"));
        }

        // Resolved per call so Samsa::fake() captures bus messages too.
        $meta = $this->container->make('samsa')->driver($this->driver)->produce($this->topic, $payload, $key, eventType: $type);

        return $envelope->with(new TransportMessageIdStamp("{$meta->partition}:{$meta->offset}"));
    }

    /**
     * @return list<Envelope>
     */
    public function get(): iterable
    {
        $this->rebalanceIfDue();

        $log = $this->client()->storage()->log();
        $envelopes = [];

        foreach ($this->partitions()->owned() as $partition) {
            foreach ($log->read($this->topic, $partition, $this->positions[$partition] ?? null, $this->batchSize) as $record) {
                $this->positions[$partition] = $record->offset;
                $this->inFlight[$partition][$record->offset] = false;

                try {
                    $envelopes[] = $this->decode($record);
                } catch (MessageDecodingFailedException $e) {
                    $this->markDone($partition, $record->offset);

                    throw $e;
                }
            }

            if ($envelopes !== []) {
                break; // one partition per call keeps batches ordered and small
            }
        }

        return $envelopes;
    }

    public function ack(Envelope $envelope): void
    {
        $this->settle($envelope);
    }

    public function reject(Envelope $envelope): void
    {
        $this->settle($envelope);
    }

    public function getMessageCount(): int
    {
        return $this->client()->stats()->lag($this->group, $this->topic);
    }

    /**
     * @return iterable<Envelope> pending messages, without claiming anything
     */
    public function all(?int $limit = null): iterable
    {
        $client = $this->client();
        $config = $client->storage()->log()->topic($this->topic);
        $remaining = $limit ?? PHP_INT_MAX;

        for ($p = 0; $config !== null && $p < $config->partitions && $remaining > 0; $p++) {
            $committed = $client->storage()->offsets()->committed($this->group, $this->topic, $p);

            foreach ($client->storage()->log()->read($this->topic, $p, $committed, min($remaining, 1000)) as $record) {
                $remaining--;

                yield $this->decode($record);
            }
        }
    }

    public function find(mixed $id): ?Envelope
    {
        if (! is_string($id) || preg_match('/^(\d+):(\d+)$/', $id, $m) !== 1) {
            return null;
        }

        $log = $this->client()->storage()->log();
        $records = $log->read($this->topic, (int) $m[1], $log->offsetBefore($this->topic, (int) $m[1], $m[2]), 1);

        return isset($records[0]) && $records[0]->offset === $m[2] ? $this->decode($records[0]) : null;
    }

    public function releaseLeases(): void
    {
        $this->partitions?->releaseAll();
        $this->partitions = null;
        $this->positions = [];
        $this->inFlight = [];
    }

    private function settle(Envelope $envelope): void
    {
        $id = (string) $envelope->last(TransportMessageIdStamp::class)?->id;

        if (preg_match('/^(\d+):(\d+)$/', $id, $m) === 1) {
            $this->markDone((int) $m[1], $m[2]);
        }
    }

    /**
     * Commit only over a contiguous run of finished records, so an envelope that
     * is still in flight is never skipped.
     */
    private function markDone(int $partition, string $offset): void
    {
        if (! isset($this->inFlight[$partition][$offset])) {
            return;
        }

        $this->inFlight[$partition][$offset] = true;
        $commit = null;

        foreach ($this->inFlight[$partition] as $pending => $done) {
            if (! $done) {
                break;
            }

            $commit = (string) $pending;
            unset($this->inFlight[$partition][$pending]);
        }

        if ($commit === null) {
            return;
        }

        try {
            $this->client()->storage()->offsets()->commit($this->group, $this->topic, $partition, $commit, $this->memberId);
        } catch (LeaseLostException) {
            $this->partitions()->forget($partition);
            unset($this->positions[$partition], $this->inFlight[$partition]);
        }
    }

    private function rebalanceIfDue(): void
    {
        if (microtime(true) - $this->lastRebalance < max(1, intdiv($this->leaseSeconds, 3))) {
            return;
        }

        $changes = $this->partitions()->rebalance();

        foreach ([...$changes['revoked'], ...$changes['lost']] as $partition) {
            unset($this->positions[$partition], $this->inFlight[$partition]);
        }

        foreach ($this->partitions()->owned() as $partition) {
            // Keep positions of partitions with records in flight; others restart from the commit.
            if (($this->inFlight[$partition] ?? []) === []) {
                $this->positions[$partition] = $this->client()->storage()->offsets()->committed($this->group, $this->topic, $partition);
            }
        }

        $this->lastRebalance = microtime(true);
    }

    private function decode(Record $record): Envelope
    {
        $envelope = $this->serializer->decode([
            'body' => $record->payload['body'] ?? '',
            'headers' => (array) ($record->payload['headers'] ?? []),
        ]);

        return $envelope->with(new TransportMessageIdStamp("{$record->partition}:{$record->offset}"));
    }

    private function partitionKey(Envelope $envelope): ?string
    {
        $stamp = $envelope->last(PartitionKeyStamp::class);

        if ($stamp !== null) {
            return $stamp->key;
        }

        $message = $envelope->getMessage();

        return $message instanceof HasPartitionKey ? $message->partitionKey() : null;
    }

    private function partitions(): PartitionManager
    {
        return $this->partitions ??= new PartitionManager(
            $this->client()->storage(),
            $this->group,
            $this->topic,
            $this->memberId,
            new WorkerOptions(leaseSeconds: max(2, $this->leaseSeconds), heartbeatSeconds: max(1, intdiv($this->leaseSeconds, 3))),
        );
    }

    private function client(): Client
    {
        return $this->container->make(SamsaManager::class)->driver($this->driver);
    }
}
