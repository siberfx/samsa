<?php

namespace Siberfx\Samsa\Drivers\Redis;

use Illuminate\Redis\Connections\PhpRedisConnection;
use Illuminate\Redis\Connections\PredisConnection;

/**
 * Raw commands through a Laravel Redis connection (phpredis or predis), plus
 * key naming. Raw commands bypass the connection's own key prefix, so Samsa's
 * keys are exactly `{prefix}...` whatever the client options are.
 *
 * @internal
 */
final class RedisConnection
{
    public function __construct(
        private readonly PhpRedisConnection|PredisConnection $redis,
        private readonly string $prefix = 'samsa:',
        private readonly int $dedupTtl = 86400,
    ) {}

    public function raw(string ...$arguments): mixed
    {
        return $this->redis->executeRaw($arguments);
    }

    /**
     * @param  list<string>  $keys
     * @param  list<string|int|float>  $arguments
     */
    public function script(string $lua, array $keys, array $arguments = []): mixed
    {
        return $this->raw('EVAL', $lua, (string) count($keys), ...$keys, ...array_map('strval', $arguments));
    }

    public function dedupTtl(): int
    {
        return $this->dedupTtl;
    }

    public function prefix(): string
    {
        return $this->prefix;
    }

    public function stream(string $topic, int $partition): string
    {
        return $this->prefix.'log:'.self::token($topic).':'.$partition;
    }

    public function topicsKey(): string
    {
        return $this->prefix.'topics';
    }

    public function offsetsKey(): string
    {
        return $this->prefix.'offsets';
    }

    public function leasesKey(string $group, string $topic): string
    {
        return $this->prefix.'leases:'.self::token($group).':'.self::token($topic);
    }

    public function membersKey(string $group, string $topic): string
    {
        return $this->prefix.'members:'.self::token($group).':'.self::token($topic);
    }

    public function dedupKey(string $topic, string $dedupKey): string
    {
        return $this->prefix.'dedup:'.self::token($topic).':'.hash('xxh128', $dedupKey);
    }

    public function deadLettersKey(): string
    {
        return $this->prefix.'dlq';
    }

    public function delayedKey(): string
    {
        return $this->prefix.'delayed';
    }

    public function delayedDataKey(): string
    {
        return $this->prefix.'delayed:data';
    }

    /**
     * Every key matching a pattern under the prefix (SCAN, never KEYS).
     *
     * @return list<string>
     */
    public function scan(string $pattern): array
    {
        $cursor = '0';
        $keys = [];

        do {
            [$cursor, $batch] = $this->raw('SCAN', $cursor, 'MATCH', $this->prefix.$pattern, 'COUNT', '500');
            array_push($keys, ...(array) $batch);
        } while ((string) $cursor !== '0');

        return array_values(array_unique(array_map('strval', $keys)));
    }

    /**
     * Key segments are separated by ":" and fields by "|": names that aren't
     * plain [A-Za-z0-9_.-] are hex-encoded behind a "~".
     */
    public static function token(string $name): string
    {
        return preg_match('/^[A-Za-z0-9_.-]+$/', $name) === 1 ? $name : '~'.bin2hex($name);
    }

    public static function untoken(string $token): string
    {
        return str_starts_with($token, '~') ? (string) hex2bin(substr($token, 1)) : $token;
    }

    /**
     * XRANGE/XREVRANGE replies as [id => [field => value]].
     *
     * @return array<string, array<string, string>>
     */
    public static function entries(mixed $reply): array
    {
        $entries = [];

        foreach ((array) $reply as $entry) {
            [$id, $flat] = $entry;
            $fields = [];

            for ($i = 0; $i + 1 < count($flat); $i += 2) {
                $fields[(string) $flat[$i]] = (string) $flat[$i + 1];
            }

            $entries[(string) $id] = $fields;
        }

        return $entries;
    }
}
