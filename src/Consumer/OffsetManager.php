<?php

namespace Siberfx\Samsa\Consumer;

use DateTimeInterface;
use InvalidArgumentException;
use Siberfx\Samsa\Contracts\StorageDriver;
use Siberfx\Samsa\Data\TopicConfig;
use Siberfx\Samsa\Exceptions\InvalidTopicException;
use Siberfx\Samsa\Exceptions\TopicNotFoundException;

/**
 * Inspect and move one group's positions on one topic.
 */
final class OffsetManager
{
    public function __construct(
        private readonly StorageDriver $driver,
        private readonly string $group,
        private readonly string $topic,
    ) {}

    public function committed(int $partition): ?string
    {
        return $this->driver->offsets()->committed($this->group, $this->topic, $partition);
    }

    /**
     * @return array<int, ?string> partition => committed offset
     */
    public function all(): array
    {
        $positions = [];

        foreach ($this->partitions(null) as $partition) {
            $positions[$partition] = $this->committed($partition);
        }

        return $positions;
    }

    /**
     * Resolve a seek target without applying it.
     *
     * Targets: earliest | latest | offset:N (start at N) | time:ISO-8601 | group:NAME
     *
     * @return array<int, ?string> partition => new committed offset
     */
    public function plan(string $target, ?int $partition = null): array
    {
        $log = $this->driver->log();
        [$kind, $value] = array_pad(explode(':', $target, 2), 2, null);

        $plan = [];

        foreach ($this->partitions($partition) as $p) {
            $plan[$p] = match ($kind) {
                'earliest' => null,
                'latest' => $log->latestOffset($this->topic, $p),
                'offset' => $partition === null
                    ? throw new InvalidArgumentException('Seeking to an offset requires a partition.')
                    : $log->offsetBefore($this->topic, $p, (string) $value),
                'time' => $log->offsetBeforeTime($this->topic, $p, new \DateTimeImmutable((string) $value)),
                'group' => $this->driver->offsets()->committed((string) $value, $this->topic, $p),
                default => throw new InvalidArgumentException("Unknown seek target [{$target}]. Use earliest, latest, offset:N, time:ISO-8601 or group:NAME."),
            };
        }

        return $plan;
    }

    /**
     * @param  array<int, ?string>  $plan
     */
    public function apply(array $plan): void
    {
        foreach ($plan as $partition => $offset) {
            $this->driver->offsets()->seek($this->group, $this->topic, $partition, $offset);
        }
    }

    public function seek(string $target, ?int $partition = null): void
    {
        $this->apply($this->plan($target, $partition));
    }

    public function seekToEarliest(?int $partition = null): void
    {
        $this->seek('earliest', $partition);
    }

    public function seekToLatest(?int $partition = null): void
    {
        $this->seek('latest', $partition);
    }

    public function seekToTimestamp(DateTimeInterface $time, ?int $partition = null): void
    {
        $this->seek('time:'.$time->format(DateTimeInterface::RFC3339_EXTENDED), $partition);
    }

    /**
     * Start consuming the partition at $offset (the record at $offset is delivered next).
     */
    public function seekToOffset(int $partition, string $offset): void
    {
        $this->seek("offset:{$offset}", $partition);
    }

    /**
     * @return list<int>
     */
    private function partitions(?int $partition): array
    {
        $config = $this->config();

        if ($partition === null) {
            return range(0, $config->partitions - 1);
        }

        if ($partition < 0 || $partition >= $config->partitions) {
            throw InvalidTopicException::invalidPartition($this->topic, $partition);
        }

        return [$partition];
    }

    private function config(): TopicConfig
    {
        return $this->driver->log()->topic($this->topic) ?? throw TopicNotFoundException::named($this->topic);
    }
}
