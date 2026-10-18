<?php

namespace Siberfx\Samsa\Drivers\Redis;

use Carbon\CarbonImmutable;
use Siberfx\Samsa\Contracts\LeaseStore;
use Siberfx\Samsa\Data\LeaseInfo;

/**
 * Leases live in one hash per (group, topic): field = partition, value =
 * "member|until". Acquire, release and the fenced commit are Lua scripts, so
 * each check-and-write is atomic.
 */
final class RedisLeaseStore implements LeaseStore
{
    public const ACQUIRE = <<<'LUA'
local current = redis.call('HGET', KEYS[1], ARGV[1])
if current then
    local sep = string.find(current, '|', 1, true)
    local member = string.sub(current, 1, sep - 1)
    local untilAt = tonumber(string.sub(current, sep + 1)) or 0
    if member ~= '' and member ~= ARGV[2] and untilAt > tonumber(ARGV[3]) then return 0 end
end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2] .. '|' .. ARGV[4])
return 1
LUA;

    private const RELEASE = <<<'LUA'
local current = redis.call('HGET', KEYS[1], ARGV[1])
if current and string.sub(current, 1, string.len(ARGV[2]) + 1) == ARGV[2] .. '|' then
    redis.call('HSET', KEYS[1], ARGV[1], '|0')
end
return 1
LUA;

    public function __construct(private readonly RedisConnection $redis) {}

    public function acquire(string $group, string $topic, int $partition, string $memberId, int $ttlSeconds): bool
    {
        $now = $this->now();

        return (int) $this->redis->script(self::ACQUIRE, [$this->redis->leasesKey($group, $topic)], [
            (string) $partition, $memberId, $this->format($now), $this->format($now + $ttlSeconds),
        ]) === 1;
    }

    public function release(string $group, string $topic, int $partition, string $memberId): void
    {
        $this->redis->script(self::RELEASE, [$this->redis->leasesKey($group, $topic)], [(string) $partition, $memberId]);
    }

    public function leases(string $group, string $topic): array
    {
        $flat = (array) $this->redis->raw('HGETALL', $this->redis->leasesKey($group, $topic));
        $leases = [];

        for ($i = 0; $i + 1 < count($flat); $i += 2) {
            $partition = (int) $flat[$i];
            [$member, $until] = explode('|', (string) $flat[$i + 1], 2);

            $leases[$partition] = new LeaseInfo(
                $partition,
                $member === '' ? null : $member,
                $member === '' ? null : CarbonImmutable::createFromTimestampUTC((float) $until),
            );
        }

        ksort($leases);

        return $leases;
    }

    public function heartbeat(string $group, string $topic, string $memberId, int $ttlSeconds): void
    {
        $this->redis->raw('ZADD', $this->redis->membersKey($group, $topic), $this->format($this->now() + $ttlSeconds), $memberId);
    }

    public function members(string $group, string $topic): array
    {
        $members = array_map('strval', (array) $this->redis->raw('ZRANGEBYSCORE', $this->redis->membersKey($group, $topic), '('.$this->format($this->now()), '+inf'));
        sort($members);

        return $members;
    }

    public function leave(string $group, string $topic, string $memberId): void
    {
        $this->redis->raw('ZREM', $this->redis->membersKey($group, $topic), $memberId);
    }

    /**
     * Carbon's clock, so tests can move time; workers should run NTP-synced clocks.
     */
    public function now(): float
    {
        return (float) CarbonImmutable::now()->format('U.u');
    }

    public function format(float $timestamp): string
    {
        return number_format($timestamp, 6, '.', '');
    }
}
