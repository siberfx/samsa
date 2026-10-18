<?php

namespace Siberfx\Samsa\Bridge;

use Illuminate\Contracts\Broadcasting\ShouldBroadcast;
use Illuminate\Contracts\Container\Container;
use InvalidArgumentException;
use ReflectionClass;
use Siberfx\Samsa\Attributes\PublishTo;
use Siberfx\Samsa\Contracts\ToSamsaPayload;
use Siberfx\Samsa\Data\RecordMetadata;
use Stringable;

/**
 * The event bridge: produces events marked with #[PublishTo] (and optionally
 * every ShouldBroadcast event) when Laravel dispatches them.
 */
final class EventPublisher
{
    /** @var array<class-string, list<PublishTo>> */
    private array $targets = [];

    /**
     * @param  array{forward_broadcast_events?: bool, broadcast_topic?: string}  $config
     */
    public function __construct(
        private readonly Container $app,
        private readonly PayloadNormalizer $normalizer,
        private readonly array $config = [],
    ) {}

    /**
     * Wildcard listener: called for every dispatched event.
     *
     * @param  array<int, mixed>  $payload
     */
    public function handleDispatched(string $eventName, array $payload): void
    {
        $event = $payload[0] ?? null;

        // Only object events, whose name is their class (not "eloquent.saved: ..." etc.).
        if (! is_object($event) || $event::class !== $eventName || str_starts_with($eventName, 'Siberfx\\Samsa\\Events\\')) {
            return;
        }

        if ($this->targets($event) !== []) {
            $this->publish($event);

            return;
        }

        if (($this->config['forward_broadcast_events'] ?? false) && $event instanceof ShouldBroadcast) {
            $this->publishBroadcast($event);
        }
    }

    /**
     * @return list<RecordMetadata> records produced immediately (deferred ones are not included)
     */
    public function publish(object $event, ?string $topic = null, string|int|null $key = null): array
    {
        $targets = $topic !== null
            ? [new PublishTo($topic, afterCommit: false)]
            : $this->targets($event);

        if ($targets === []) {
            throw new InvalidArgumentException(sprintf('[%s] has no #[PublishTo] attribute; pass a topic to Samsa::publish().', $event::class));
        }

        $payload = $event instanceof ToSamsaPayload ? $event->toSamsaPayload() : $this->normalizer->properties($event);
        $headers = method_exists($event, 'samsaHeaders') ? (array) $event->samsaHeaders() : [];
        $produced = [];

        foreach ($targets as $target) {
            $recordKey = $key ?? ($target->key === null ? null : $this->keyFrom($event, $target->key));

            $produced[] = $this->produce(
                $target,
                $payload,
                $recordKey === null ? null : (string) $recordKey,
                $headers,
                $target->eventType ?? $event::class,
            );
        }

        return array_values(array_filter($produced));
    }

    /**
     * @return list<PublishTo>
     */
    public function targets(object $event): array
    {
        return $this->targets[$event::class] ??= array_map(
            fn ($attribute) => $attribute->newInstance(),
            (new ReflectionClass($event))->getAttributes(PublishTo::class),
        );
    }

    private function publishBroadcast(ShouldBroadcast $event): void
    {
        $channels = (array) $event->broadcastOn();
        $first = $channels[0] ?? null;
        $key = is_string($first) || $first instanceof Stringable ? (string) $first : null;

        $payload = method_exists($event, 'broadcastWith') ? (array) $event->broadcastWith() : $this->normalizer->properties($event);
        $type = method_exists($event, 'broadcastAs') ? (string) $event->broadcastAs() : $event::class;

        $this->produce(
            new PublishTo((string) ($this->config['broadcast_topic'] ?? 'broadcasts')),
            $this->normalizer->normalize($payload),
            $key,
            [],
            $type,
        );
    }

    /**
     * @param  array<string, mixed>  $payload
     * @param  array<string, scalar|null>  $headers
     */
    private function produce(PublishTo $target, array $payload, ?string $key, array $headers, string $eventType): ?RecordMetadata
    {
        // Resolve on every call so Samsa::fake() is honoured.
        $samsa = $this->app->make('samsa')->driver($target->driver);

        if ($target->afterCommit) {
            $samsa->afterCommit()->produce($target->topic, $payload, $key, $headers, eventType: $eventType);

            return null;
        }

        return $samsa->produce($target->topic, $payload, $key, $headers, eventType: $eventType);
    }

    private function keyFrom(object $event, string $key): mixed
    {
        if (method_exists($event, $key)) {
            return $event->{$key}();
        }

        if (property_exists($event, $key)) {
            // Enums become their value, models their key.
            $value = $this->normalizer->normalize($event->{$key});

            return is_array($value) ? ($value['id'] ?? json_encode($value)) : $value;
        }

        throw new InvalidArgumentException(sprintf('[%s] has no property or method [%s] to use as the Samsa key.', $event::class, $key));
    }
}
