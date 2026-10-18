<?php

namespace Siberfx\Samsa\Drivers\Redis;

use Carbon\CarbonImmutable;
use DateTimeInterface;
use Siberfx\Samsa\Contracts\LogStore;
use Siberfx\Samsa\Contracts\Serializer;
use Siberfx\Samsa\Data\OutgoingRecord;
use Siberfx\Samsa\Data\Record;
use Siberfx\Samsa\Data\RecordMetadata;
use Siberfx\Samsa\Data\RetentionPolicy;
use Siberfx\Samsa\Data\TopicConfig;
use Siberfx\Samsa\Exceptions\InvalidTopicException;
use Siberfx\Samsa\Exceptions\TopicNotFoundException;

/**
 * One Redis stream per partition ({prefix}log:{topic}:{N}); offsets are entry
 * IDs ("1729270000000-0"), compared by the driver only.
 */
final class RedisLogStore implements LogStore
{
    private const SCAN_CHUNK = 1000;

    /**
     * Dedup check + XADD in one atomic step. Returns {"partition|id", duplicate}.
     */
    private const APPEND = <<<'LUA'
if KEYS[2] ~= '' then
    local existing = redis.call('GET', KEYS[2])
    if existing then return {existing, 1} end
end
local args = {'XADD', KEYS[1]}
if ARGV[3] ~= '' then
    table.insert(args, 'MAXLEN'); table.insert(args, '~'); table.insert(args, ARGV[3])
end
table.insert(args, '*')
for i = 4, #ARGV do table.insert(args, ARGV[i]) end
local id = redis.call(unpack(args))
local location = ARGV[2] .. '|' .. id
if KEYS[2] ~= '' then redis.call('SET', KEYS[2], location, 'EX', ARGV[1]) end
return {location, 0}
LUA;

    public function __construct(
        private readonly RedisConnection $redis,
        private readonly Serializer $serializer,
    ) {}

    public function ensureTopic(TopicConfig $topic): TopicConfig
    {
        $existing = $this->topic($topic->name);
        $config = new TopicConfig($topic->name, $existing->partitions ?? $topic->partitions, $topic->retention);

        $this->save($config);

        return $config;
    }

    public function topic(string $name): ?TopicConfig
    {
        $value = $this->redis->raw('HGET', $this->redis->topicsKey(), $name);

        if ($value === null || $value === false) {
            return null;
        }

        $data = (array) json_decode((string) $value, true);

        return new TopicConfig($name, (int) $data['partitions'], RetentionPolicy::fromArray((array) ($data['retention'] ?? [])));
    }

    public function topics(): array
    {
        $names = array_map('strval', (array) $this->redis->raw('HKEYS', $this->redis->topicsKey()));
        sort($names);

        return array_values(array_filter(array_map(fn (string $name) => $this->topic($name), $names)));
    }

    public function growPartitions(string $topic, int $partitions): TopicConfig
    {
        $config = $this->topic($topic) ?? throw TopicNotFoundException::named($topic);

        if ($partitions < $config->partitions) {
            throw InvalidTopicException::cannotShrink($topic, $config->partitions, $partitions);
        }

        $grown = new TopicConfig($topic, $partitions, $config->retention);
        $this->save($grown);

        return $grown;
    }

    public function deleteTopic(string $topic): void
    {
        $config = $this->topic($topic);

        for ($p = 0; $config !== null && $p < $config->partitions; $p++) {
            $this->redis->raw('DEL', $this->redis->stream($topic, $p));
        }

        $token = RedisConnection::token($topic);

        foreach ((array) $this->redis->raw('HKEYS', $this->redis->offsetsKey()) as $field) {
            if (explode('|', (string) $field)[1] === $token) {
                $this->redis->raw('HDEL', $this->redis->offsetsKey(), (string) $field);
            }
        }

        foreach ([...$this->redis->scan('leases:*:'.$token), ...$this->redis->scan('members:*:'.$token), ...$this->redis->scan('dedup:'.$token.':*')] as $key) {
            $this->redis->raw('DEL', $key);
        }

        $this->redis->raw('HDEL', $this->redis->topicsKey(), $topic);
    }

    public function append(string $topic, int $partition, OutgoingRecord $record): RecordMetadata
    {
        $config = $this->requirePartition($topic, $partition);
        $retention = $config->retention;
        $maxLen = ! $retention->respectSlowestConsumer && $retention->maxRecordsPerPartition !== null
            ? (string) $retention->maxRecordsPerPartition
            : '';

        $fields = [
            'p', $this->serializer->encode($record->payload),
            'h', $this->serializer->encode($record->headers),
            'c', CarbonImmutable::now('UTC')->format('Y-m-d\TH:i:s.uP'),
        ];

        if ($record->key !== null) {
            array_push($fields, 'k', $record->key);
        }

        if ($record->eventType !== null) {
            array_push($fields, 't', $record->eventType);
        }

        [$location] = $this->redis->script(self::APPEND, [
            $this->redis->stream($topic, $partition),
            $record->dedupKey === null ? '' : $this->redis->dedupKey($topic, $record->dedupKey),
        ], [$this->redis->dedupTtl(), $partition, $maxLen, ...$fields]);

        [$storedPartition, $id] = explode('|', (string) $location, 2);

        return new RecordMetadata($topic, (int) $storedPartition, $id);
    }

    public function appendBatch(string $topic, int $partition, array $records): array
    {
        return array_map(fn (OutgoingRecord $record) => $this->append($topic, $partition, $record), $records);
    }

    public function read(string $topic, int $partition, ?string $afterOffset, int $limit): array
    {
        $this->requirePartition($topic, $partition);

        $entries = RedisConnection::entries($this->redis->raw(
            'XRANGE',
            $this->redis->stream($topic, $partition),
            $afterOffset === null ? '-' : '('.$afterOffset,
            '+',
            'COUNT',
            (string) $limit,
        ));

        $records = [];

        foreach ($entries as $id => $fields) {
            $records[] = $this->hydrate($topic, $partition, $id, $fields);
        }

        return $records;
    }

    public function latestOffset(string $topic, int $partition): ?string
    {
        $this->requirePartition($topic, $partition);

        $entries = RedisConnection::entries($this->redis->raw('XREVRANGE', $this->redis->stream($topic, $partition), '+', '-', 'COUNT', '1'));

        return array_key_first($entries);
    }

    public function offsetBeforeTime(string $topic, int $partition, DateTimeInterface $time): ?string
    {
        $first = $this->firstAtOrAfter($topic, $partition, CarbonImmutable::instance($time));

        return $first === null ? $this->latestOffset($topic, $partition) : $this->offsetBefore($topic, $partition, $first);
    }

    /**
     * The entry before $offset in the partition, or null when $offset is the first.
     */
    public function offsetBefore(string $topic, int $partition, string $offset): ?string
    {
        $previous = RedisConnection::entries($this->redis->raw('XREVRANGE', $this->redis->stream($topic, $partition), '('.$offset, '-', 'COUNT', '1'));

        return array_key_first($previous);
    }

    public function countAfter(string $topic, int $partition, ?string $afterOffset): int
    {
        $this->requirePartition($topic, $partition);
        $stream = $this->redis->stream($topic, $partition);

        if ($afterOffset === null) {
            return (int) $this->redis->raw('XLEN', $stream);
        }

        $count = 0;
        $cursor = '('.$afterOffset;

        do {
            $ids = array_keys(RedisConnection::entries($this->redis->raw('XRANGE', $stream, $cursor, '+', 'COUNT', (string) self::SCAN_CHUNK)));
            $count += count($ids);
            $cursor = $ids === [] ? null : '('.end($ids);
        } while ($cursor !== null && count($ids) === self::SCAN_CHUNK);

        return $count;
    }

    public function prune(string $topic, RetentionPolicy $policy, ?array $protectAfter = null): int
    {
        $config = $this->topic($topic) ?? throw TopicNotFoundException::named($topic);

        if ($policy->maxAgeSeconds === null && $policy->maxRecordsPerPartition === null) {
            return 0;
        }

        $deleted = 0;

        for ($partition = 0; $partition < $config->partitions; $partition++) {
            $stream = $this->redis->stream($topic, $partition);
            $keepFrom = null; // the first entry ID to keep

            if ($protectAfter !== null) {
                $protected = $protectAfter[$partition] ?? null;

                if ($protected === null) {
                    continue;
                }

                $keepFrom = $this->firstAfter($stream, $protected);
            }

            if ($policy->maxAgeSeconds !== null) {
                $byAge = $this->firstAtOrAfter($topic, $partition, CarbonImmutable::now('UTC')->subSeconds($policy->maxAgeSeconds));
                $keepFrom = $this->earliest($keepFrom, $byAge ?? $this->pastTheEnd($stream));
            }

            if ($policy->maxRecordsPerPartition !== null) {
                $last = array_keys(RedisConnection::entries($this->redis->raw('XREVRANGE', $stream, '+', '-', 'COUNT', (string) $policy->maxRecordsPerPartition)));
                $keepFrom = $this->earliest($keepFrom, $last === [] ? null : (string) end($last));
            }

            if ($keepFrom === null) {
                continue;
            }

            $before = (int) $this->redis->raw('XLEN', $stream);
            $this->redis->raw('XTRIM', $stream, 'MINID', $keepFrom);
            $deleted += $before - (int) $this->redis->raw('XLEN', $stream);
        }

        return $deleted;
    }

    private function save(TopicConfig $topic): void
    {
        $this->redis->raw('HSET', $this->redis->topicsKey(), $topic->name, (string) json_encode([
            'partitions' => $topic->partitions,
            'retention' => $topic->retention->toArray(),
        ]));
    }

    /**
     * @param  array<string, string>  $fields
     */
    private function hydrate(string $topic, int $partition, string $id, array $fields): Record
    {
        return new Record(
            topic: $topic,
            partition: $partition,
            offset: $id,
            key: $fields['k'] ?? null,
            payload: $this->serializer->decode($fields['p'] ?? '[]'),
            headers: isset($fields['h']) ? $this->serializer->decode($fields['h']) : [],
            createdAt: isset($fields['c']) ? CarbonImmutable::parse($fields['c'])->utc() : CarbonImmutable::now('UTC'),
            eventType: $fields['t'] ?? null,
        );
    }

    /**
     * ID of the partition's first entry created at or after $time (scanned in chunks).
     */
    private function firstAtOrAfter(string $topic, int $partition, CarbonImmutable $time): ?string
    {
        $stream = $this->redis->stream($topic, $partition);
        $cursor = '-';

        do {
            $entries = RedisConnection::entries($this->redis->raw('XRANGE', $stream, $cursor, '+', 'COUNT', (string) self::SCAN_CHUNK));

            foreach ($entries as $id => $fields) {
                if (isset($fields['c']) && CarbonImmutable::parse($fields['c'])->greaterThanOrEqualTo($time)) {
                    return $id;
                }
            }

            $cursor = $entries === [] ? null : '('.array_key_last($entries);
        } while ($cursor !== null && count($entries) === self::SCAN_CHUNK);

        return null;
    }

    private function firstAfter(string $stream, string $offset): string
    {
        $next = array_key_first(RedisConnection::entries($this->redis->raw('XRANGE', $stream, '('.$offset, '+', 'COUNT', '1')));

        return $next ?? $this->pastTheEnd($stream);
    }

    /**
     * An ID greater than every entry, so "keep from here" trims everything.
     */
    private function pastTheEnd(string $stream): string
    {
        $last = array_key_first(RedisConnection::entries($this->redis->raw('XREVRANGE', $stream, '+', '-', 'COUNT', '1')));

        if ($last === null) {
            return '0-1';
        }

        [$ms, $seq] = array_map('intval', explode('-', $last, 2));

        return $ms.'-'.($seq + 1);
    }

    private function earliest(?string $a, ?string $b): ?string
    {
        if ($a === null || $b === null) {
            return $a ?? $b;
        }

        return self::compare($a, $b) <= 0 ? $a : $b;
    }

    public static function compare(string $a, string $b): int
    {
        [$aMs, $aSeq] = array_map('intval', array_pad(explode('-', $a, 2), 2, '0'));
        [$bMs, $bSeq] = array_map('intval', array_pad(explode('-', $b, 2), 2, '0'));

        return [$aMs, $aSeq] <=> [$bMs, $bSeq];
    }

    private function requirePartition(string $topic, int $partition): TopicConfig
    {
        $config = $this->topic($topic) ?? throw TopicNotFoundException::named($topic);

        if ($partition < 0 || $partition >= $config->partitions) {
            throw InvalidTopicException::invalidPartition($topic, $partition);
        }

        return $config;
    }
}
