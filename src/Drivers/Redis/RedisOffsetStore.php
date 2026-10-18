<?php

namespace Siberfx\Samsa\Drivers\Redis;

use Siberfx\Samsa\Contracts\OffsetStore;
use Siberfx\Samsa\Exceptions\LeaseLostException;

/**
 * Committed offsets in one hash: field "group|topic|partition", value the offset
 * (an empty string means "from the beginning").
 */
final class RedisOffsetStore implements OffsetStore
{
    /**
     * Fenced commit: write the offset only while the member's lease is unexpired.
     */
    private const COMMIT = <<<'LUA'
local current = redis.call('HGET', KEYS[1], ARGV[1])
if not current then return 0 end
local sep = string.find(current, '|', 1, true)
local member = string.sub(current, 1, sep - 1)
local untilAt = tonumber(string.sub(current, sep + 1)) or 0
if member ~= ARGV[2] or untilAt <= tonumber(ARGV[3]) then return 0 end
redis.call('HSET', KEYS[2], ARGV[4], ARGV[5])
return 1
LUA;

    public function __construct(
        private readonly RedisConnection $redis,
        private readonly RedisLeaseStore $leases,
    ) {}

    public function committed(string $group, string $topic, int $partition): ?string
    {
        $value = $this->redis->raw('HGET', $this->redis->offsetsKey(), $this->field($group, $topic, $partition));

        return $value === null || $value === false || $value === '' ? null : (string) $value;
    }

    public function hasPosition(string $group, string $topic, int $partition): bool
    {
        return (int) $this->redis->raw('HEXISTS', $this->redis->offsetsKey(), $this->field($group, $topic, $partition)) === 1;
    }

    public function commit(string $group, string $topic, int $partition, string $offset, ?string $memberId = null): void
    {
        if ($memberId === null) {
            $this->seek($group, $topic, $partition, $offset);

            return;
        }

        $committed = (int) $this->redis->script(self::COMMIT, [$this->redis->leasesKey($group, $topic), $this->redis->offsetsKey()], [
            (string) $partition,
            $memberId,
            $this->leases->format($this->leases->now()),
            $this->field($group, $topic, $partition),
            $offset,
        ]);

        if ($committed !== 1) {
            throw LeaseLostException::for($group, $topic, $partition, $memberId);
        }
    }

    public function seek(string $group, string $topic, int $partition, ?string $offset): void
    {
        $this->redis->raw('HSET', $this->redis->offsetsKey(), $this->field($group, $topic, $partition), $offset ?? '');
    }

    public function groups(string $topic): array
    {
        $groups = array_filter($this->allGroups(), fn (array $topics) => in_array($topic, $topics, true));

        return array_map('strval', array_keys($groups));
    }

    public function allGroups(): array
    {
        $groups = [];

        foreach ((array) $this->redis->raw('HKEYS', $this->redis->offsetsKey()) as $field) {
            [$group, $topic] = explode('|', (string) $field);
            $groups[RedisConnection::untoken($group)][RedisConnection::untoken($topic)] = true;
        }

        ksort($groups);

        return array_map(function (array $topics) {
            $names = array_map('strval', array_keys($topics));
            sort($names);

            return $names;
        }, $groups);
    }

    private function field(string $group, string $topic, int $partition): string
    {
        return RedisConnection::token($group).'|'.RedisConnection::token($topic).'|'.$partition;
    }
}
