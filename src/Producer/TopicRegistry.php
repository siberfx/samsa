<?php

namespace Siberfx\Samsa\Producer;

use Siberfx\Samsa\Contracts\LogStore;
use Siberfx\Samsa\Data\RetentionPolicy;
use Siberfx\Samsa\Data\TopicConfig;
use Siberfx\Samsa\Exceptions\TopicNotFoundException;

/**
 * Resolves topic configuration for producing: a short-lived cache in front of
 * the driver, plus auto-creation from config when enabled.
 */
final class TopicRegistry
{
    private const CACHE_SECONDS = 30;

    /** @var array<string, array{config: TopicConfig, at: float}> */
    private array $cache = [];

    /**
     * @param  array{partitions?: int, retention?: array<string, mixed>}  $defaults
     * @param  array<string, array{partitions?: int, retention?: array<string, mixed>}>  $topics
     */
    public function __construct(
        private readonly LogStore $log,
        private readonly array $defaults = [],
        private readonly array $topics = [],
        private readonly bool $autoCreate = false,
    ) {}

    public function resolve(string $topic): TopicConfig
    {
        $cached = $this->cache[$topic] ?? null;

        if ($cached !== null && microtime(true) - $cached['at'] < self::CACHE_SECONDS) {
            return $cached['config'];
        }

        $config = $this->log->topic($topic);

        if ($config === null) {
            if (! $this->autoCreate) {
                throw TopicNotFoundException::named($topic);
            }

            $config = $this->log->ensureTopic($this->configured($topic));
        }

        $this->cache[$topic] = ['config' => $config, 'at' => microtime(true)];

        return $config;
    }

    public function forget(string $topic): void
    {
        unset($this->cache[$topic]);
    }

    /**
     * The topic as declared in config (or the defaults when it is not declared).
     */
    public function configured(string $topic, ?int $partitions = null, ?RetentionPolicy $retention = null): TopicConfig
    {
        $declared = $this->topics[$topic] ?? [];

        return new TopicConfig(
            $topic,
            $partitions ?? (int) ($declared['partitions'] ?? $this->defaults['partitions'] ?? 8),
            $retention ?? RetentionPolicy::fromArray([...($this->defaults['retention'] ?? []), ...($declared['retention'] ?? [])]),
        );
    }

    /**
     * @return list<TopicConfig>
     */
    public function declared(): array
    {
        return array_map(fn (string $name) => $this->configured($name), array_keys($this->topics));
    }
}
