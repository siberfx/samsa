<?php

namespace Siberfx\Samsa\Consumer;

use Closure;
use Siberfx\Samsa\Contracts\BatchHandler;
use Siberfx\Samsa\Contracts\Handler;

/**
 * group => topic => handler. Handlers are class names, instances or closures.
 */
final class SubscriptionRegistry
{
    /** @var array<string, array<string, class-string<Handler|BatchHandler>|Handler|BatchHandler|Closure>> */
    private array $subscriptions = [];

    /**
     * @param  array<string, array<string, class-string<Handler|BatchHandler>>>  $fromConfig
     */
    public function __construct(array $fromConfig = [])
    {
        foreach ($fromConfig as $group => $topics) {
            foreach ($topics as $topic => $handler) {
                $this->subscribe($group, $topic, $handler);
            }
        }
    }

    /**
     * @param  class-string<Handler|BatchHandler>|Handler|BatchHandler|Closure  $handler
     */
    public function subscribe(string $group, string $topic, string|Handler|BatchHandler|Closure $handler): void
    {
        $this->subscriptions[$group][$topic] = $handler;
    }

    /**
     * @return class-string<Handler|BatchHandler>|Handler|BatchHandler|Closure|null
     */
    public function handler(string $group, string $topic): string|Handler|BatchHandler|Closure|null
    {
        return $this->subscriptions[$group][$topic] ?? null;
    }

    /**
     * @return list<string>
     */
    public function topics(string $group): array
    {
        return array_keys($this->subscriptions[$group] ?? []);
    }

    /**
     * @return array<string, array<string, class-string<Handler|BatchHandler>|Handler|BatchHandler|Closure>>
     */
    public function all(): array
    {
        return $this->subscriptions;
    }
}
