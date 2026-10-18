<?php

namespace Siberfx\Samsa\Drivers\Nats;

use Carbon\CarbonImmutable;
use DateTimeInterface;
use Siberfx\Samsa\Contracts\LogStore;
use Siberfx\Samsa\Contracts\Serializer;
use Siberfx\Samsa\Data\OutgoingRecord;
use Siberfx\Samsa\Data\Record;
use Siberfx\Samsa\Data\RecordMetadata;
use Siberfx\Samsa\Data\RetentionPolicy;
use Siberfx\Samsa\Data\TopicConfig;
use Siberfx\Samsa\Exceptions\DriverException;
use Siberfx\Samsa\Exceptions\InvalidTopicException;
use Siberfx\Samsa\Exceptions\TopicNotFoundException;

/**
 * One JetStream stream per topic; partition N is subject {prefix}.{topic}.N and
 * offsets are stream sequences (increasing per partition, not contiguous).
 */
final class NatsLogStore implements LogStore
{
    private const H_KEY = 'Samsa-Key';

    private const H_TYPE = 'Samsa-Event-Type';

    private const H_HEADERS = 'Samsa-Headers';

    private const H_CREATED = 'Samsa-Created-At';

    /** @var array<string, TopicConfig> */
    private array $topics = [];

    public function __construct(
        private readonly NatsConnection $nats,
        private readonly NatsState $state,
        private readonly Serializer $serializer,
    ) {}

    public function ensureTopic(TopicConfig $topic): TopicConfig
    {
        $existing = $this->topic($topic->name);
        $config = new TopicConfig($topic->name, $existing->partitions ?? $topic->partitions, $topic->retention);

        $streamConfig = $this->streamConfig($config);

        if ($existing === null) {
            $this->nats->ensureStream($streamConfig);
        } else {
            $this->nats->api("STREAM.UPDATE.{$streamConfig['name']}", $streamConfig);
        }

        $this->state->put($this->topicKey($topic->name), ['partitions' => $config->partitions, 'retention' => $config->retention->toArray()]);

        return $this->topics[$topic->name] = $config;
    }

    public function topic(string $name): ?TopicConfig
    {
        $entry = $this->state->get($this->topicKey($name));

        if ($entry === null) {
            unset($this->topics[$name]);

            return null;
        }

        return $this->topics[$name] = new TopicConfig(
            $name,
            (int) $entry['value']['partitions'],
            RetentionPolicy::fromArray((array) ($entry['value']['retention'] ?? [])),
        );
    }

    public function topics(): array
    {
        $topics = [];

        foreach ($this->state->keys('topics.*') as $key) {
            $topic = $this->topic(NatsConnection::untoken(substr($key, strlen('topics.'))));

            if ($topic !== null) {
                $topics[$topic->name] = $topic;
            }
        }

        ksort($topics);

        return array_values($topics);
    }

    public function growPartitions(string $topic, int $partitions): TopicConfig
    {
        $config = $this->topic($topic) ?? throw TopicNotFoundException::named($topic);

        if ($partitions < $config->partitions) {
            throw InvalidTopicException::cannotShrink($topic, $config->partitions, $partitions);
        }

        // The stream's wildcard subject already covers every partition number.
        $grown = new TopicConfig($topic, $partitions, $config->retention);
        $this->state->put($this->topicKey($topic), ['partitions' => $partitions, 'retention' => $config->retention->toArray()]);

        return $this->topics[$topic] = $grown;
    }

    public function deleteTopic(string $topic): void
    {
        $this->nats->api("STREAM.DELETE.{$this->nats->topicStream($topic)}", [], [NatsConnection::ERR_STREAM_NOT_FOUND]);

        $token = NatsConnection::token($topic);

        foreach (['offsets.*.%s.*', 'leases.*.%s.*', 'members.*.%s.*'] as $pattern) {
            foreach ($this->state->keys(sprintf($pattern, $token)) as $key) {
                $this->state->delete($key);
            }
        }

        $this->state->delete($this->topicKey($topic));
        unset($this->topics[$topic]);
    }

    public function append(string $topic, int $partition, OutgoingRecord $record): RecordMetadata
    {
        $this->requirePartition($topic, $partition);

        $headers = [
            self::H_HEADERS => base64_encode($this->serializer->encode($record->headers)),
            self::H_CREATED => CarbonImmutable::now('UTC')->format('Y-m-d\TH:i:s.uP'),
        ];

        if ($record->key !== null) {
            $headers[self::H_KEY] = rawurlencode($record->key);
        }

        if ($record->eventType !== null) {
            $headers[self::H_TYPE] = rawurlencode($record->eventType);
        }

        if ($record->dedupKey !== null) {
            $headers['Nats-Msg-Id'] = rawurlencode($record->dedupKey);
        }

        $ack = $this->nats->publish($this->nats->partitionSubject($topic, $partition), $this->serializer->encode($record->payload), $headers);

        if ($ack === null) {
            throw new DriverException('NATS did not acknowledge the record.');
        }

        if ($ack->duplicate) {
            // Within the duplicate window: report where the original lives.
            $original = $this->nats->getMessage($this->nats->topicStream($topic), ['seq' => $ack->seq]);
            $partition = $original === null ? $partition : (int) substr($original['subject'], strrpos($original['subject'], '.') + 1);
        }

        return new RecordMetadata($topic, $partition, (string) $ack->seq);
    }

    public function appendBatch(string $topic, int $partition, array $records): array
    {
        return array_map(fn (OutgoingRecord $record) => $this->append($topic, $partition, $record), $records);
    }

    public function read(string $topic, int $partition, ?string $afterOffset, int $limit): array
    {
        $this->requirePartition($topic, $partition);

        $stream = $this->nats->topicStream($topic);
        $subject = $this->nats->partitionSubject($topic, $partition);
        $sequence = $afterOffset === null ? 1 : (int) $afterOffset + 1;
        $records = [];

        while (count($records) < $limit) {
            $message = $this->nats->getMessage($stream, ['seq' => $sequence, 'next_by_subj' => $subject]);

            if ($message === null) {
                break;
            }

            $records[] = $this->hydrate($topic, $partition, $message);
            $sequence = $message['seq'] + 1;
        }

        return $records;
    }

    public function latestOffset(string $topic, int $partition): ?string
    {
        $this->requirePartition($topic, $partition);

        $message = $this->nats->getMessage($this->nats->topicStream($topic), ['last_by_subj' => $this->nats->partitionSubject($topic, $partition)]);

        return $message === null ? null : (string) $message['seq'];
    }

    public function offsetBeforeTime(string $topic, int $partition, DateTimeInterface $time): ?string
    {
        $first = $this->firstAtOrAfter($topic, $partition, CarbonImmutable::instance($time));

        if ($first === null) {
            return $this->latestOffset($topic, $partition);
        }

        return $first <= 1 ? null : (string) ($first - 1);
    }

    /**
     * Offsets are positions: reading "after N - 1" starts at N, whichever
     * partition sequence N - 1 belongs to.
     */
    public function offsetBefore(string $topic, int $partition, string $offset): ?string
    {
        return (int) $offset <= 1 ? null : (string) ((int) $offset - 1);
    }

    public function countAfter(string $topic, int $partition, ?string $afterOffset): int
    {
        $this->requirePartition($topic, $partition);

        $stream = $this->nats->topicStream($topic);
        $config = [
            'filter_subject' => $this->nats->partitionSubject($topic, $partition),
            'ack_policy' => 'none',
            'mem_storage' => true,
            'inactive_threshold' => 10_000_000_000,
        ];

        $config += $afterOffset === null
            ? ['deliver_policy' => 'all']
            : ['deliver_policy' => 'by_start_sequence', 'opt_start_seq' => (int) $afterOffset + 1];

        $consumer = $this->nats->api("CONSUMER.CREATE.{$stream}", ['stream_name' => $stream, 'config' => $config]);
        $pending = (int) ($consumer->num_pending ?? 0);

        if (isset($consumer->name)) {
            $this->nats->api("CONSUMER.DELETE.{$stream}.{$consumer->name}", [], [10014]);
        }

        return $pending;
    }

    public function prune(string $topic, RetentionPolicy $policy, ?array $protectAfter = null): int
    {
        $config = $this->topic($topic) ?? throw TopicNotFoundException::named($topic);

        if ($policy->maxAgeSeconds === null && $policy->maxRecordsPerPartition === null) {
            return 0;
        }

        $stream = $this->nats->topicStream($topic);
        $purged = 0;

        for ($partition = 0; $partition < $config->partitions; $partition++) {
            $subject = $this->nats->partitionSubject($topic, $partition);
            $boundary = PHP_INT_MAX; // purge every sequence below this

            if ($protectAfter !== null) {
                $protected = $protectAfter[$partition] ?? null;

                if ($protected === null) {
                    continue;
                }

                $boundary = (int) $protected + 1;
            }

            if ($policy->maxAgeSeconds !== null) {
                $cutoff = CarbonImmutable::now('UTC')->subSeconds($policy->maxAgeSeconds);
                $boundary = min($boundary, $this->firstAtOrAfter($topic, $partition, $cutoff) ?? $this->nextSequence($stream));
            }

            if ($policy->maxRecordsPerPartition !== null) {
                $boundary = min($boundary, $this->keepFrom($topic, $partition, $policy->maxRecordsPerPartition));
            }

            if ($boundary === PHP_INT_MAX || $boundary <= 1) {
                continue;
            }

            $result = $this->nats->api("STREAM.PURGE.{$stream}", ['filter' => $subject, 'seq' => $boundary]);
            $purged += (int) ($result->purged ?? 0);
        }

        return $purged;
    }

    /**
     * @return array<string, mixed>
     */
    private function streamConfig(TopicConfig $topic): array
    {
        $retention = $topic->retention;
        $native = ! $retention->respectSlowestConsumer;
        $maxAge = $native && $retention->maxAgeSeconds !== null ? $retention->maxAgeSeconds * 1_000_000_000 : 0;

        return [
            'name' => $this->nats->topicStream($topic->name),
            'subjects' => [$this->nats->topicSubjects($topic->name)],
            'retention' => 'limits',
            'storage' => $this->nats->storage(),
            'num_replicas' => $this->nats->replicas(),
            'discard' => 'old',
            'max_age' => $maxAge,
            'max_msgs_per_subject' => $native && $retention->maxRecordsPerPartition !== null ? $retention->maxRecordsPerPartition : -1,
            // JetStream requires the dedup window to fit inside max_age.
            'duplicate_window' => $maxAge > 0 ? min($maxAge, $this->nats->duplicateWindowNanos()) : $this->nats->duplicateWindowNanos(),
            'allow_direct' => true,
        ];
    }

    /**
     * @param  array{seq: int, subject: string, headers: array<string, string>, data: string}  $message
     */
    private function hydrate(string $topic, int $partition, array $message): Record
    {
        $headers = $message['headers'];

        return new Record(
            topic: $topic,
            partition: $partition,
            offset: (string) $message['seq'],
            key: isset($headers[self::H_KEY]) ? rawurldecode($headers[self::H_KEY]) : null,
            payload: $this->serializer->decode($message['data']),
            headers: isset($headers[self::H_HEADERS]) ? $this->serializer->decode((string) base64_decode($headers[self::H_HEADERS])) : [],
            createdAt: $this->createdAt($headers),
            eventType: isset($headers[self::H_TYPE]) ? rawurldecode($headers[self::H_TYPE]) : null,
        );
    }

    /**
     * @param  array<string, string>  $headers
     */
    private function createdAt(array $headers): CarbonImmutable
    {
        return isset($headers[self::H_CREATED])
            ? CarbonImmutable::parse($headers[self::H_CREATED])->utc()
            : CarbonImmutable::now('UTC');
    }

    /**
     * Sequence of the partition's first record created at or after $time (binary search).
     */
    private function firstAtOrAfter(string $topic, int $partition, CarbonImmutable $time): ?int
    {
        $stream = $this->nats->topicStream($topic);
        $subject = $this->nats->partitionSubject($topic, $partition);
        $low = 1;
        $high = $this->nextSequence($stream);
        $found = null;

        while ($low < $high) {
            $middle = intdiv($low + $high, 2);
            $message = $this->nats->getMessage($stream, ['seq' => $middle, 'next_by_subj' => $subject]);

            if ($message === null || $message['seq'] >= $high) {
                $high = $middle;

                continue;
            }

            if ($this->createdAt($message['headers'])->greaterThanOrEqualTo($time)) {
                $found = $message['seq'];
                $high = $middle;
            } else {
                $low = $message['seq'] + 1;
            }
        }

        return $found;
    }

    /**
     * The sequence from which the last $keep records of the partition start.
     */
    private function keepFrom(string $topic, int $partition, int $keep): int
    {
        $low = 0;
        $high = $this->nextSequence($this->nats->topicStream($topic));

        // Smallest position whose remainder holds at most $keep records.
        while ($low < $high) {
            $middle = intdiv($low + $high, 2);

            if ($this->countAfter($topic, $partition, $middle === 0 ? null : (string) $middle) <= $keep) {
                $high = $middle;
            } else {
                $low = $middle + 1;
            }
        }

        return $low + 1;
    }

    private function nextSequence(string $stream): int
    {
        $info = $this->nats->api("STREAM.INFO.{$stream}", [], [NatsConnection::ERR_STREAM_NOT_FOUND]);

        return (int) ($info->state->last_seq ?? 0) + 1;
    }

    private function requirePartition(string $topic, int $partition): void
    {
        $config = $this->topics[$topic] ?? $this->topic($topic) ?? throw TopicNotFoundException::named($topic);

        if ($partition < 0 || $partition >= $config->partitions) {
            // The cached count may be stale after growPartitions() elsewhere.
            $config = $this->topic($topic) ?? throw TopicNotFoundException::named($topic);

            if ($partition < 0 || $partition >= $config->partitions) {
                throw InvalidTopicException::invalidPartition($topic, $partition);
            }
        }
    }

    private function topicKey(string $topic): string
    {
        return 'topics.'.NatsConnection::token($topic);
    }
}
