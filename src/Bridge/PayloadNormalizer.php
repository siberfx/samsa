<?php

namespace Siberfx\Samsa\Bridge;

use BackedEnum;
use DateTimeInterface;
use Illuminate\Contracts\Support\Arrayable;
use Illuminate\Database\Eloquent\Model;
use JsonSerializable;
use ReflectionObject;
use ReflectionProperty;
use Siberfx\Samsa\Contracts\ToSamsaPayload;
use Siberfx\Samsa\Exceptions\SerializationException;
use UnitEnum;

/**
 * Turns an event's public properties into a JSON-safe payload. Models become
 * identifiers (never the whole model), so payloads stay small and stable.
 */
final class PayloadNormalizer
{
    /**
     * Properties added by Laravel traits that are never part of the payload.
     */
    private const IGNORED = ['socket', 'connection', 'queue', 'delay', 'afterCommit', 'middleware', 'chained', 'chainConnection', 'chainQueue', 'chainCatchCallbacks'];

    /**
     * @return array<string, mixed>
     */
    public function properties(object $event): array
    {
        $payload = [];

        foreach ((new ReflectionObject($event))->getProperties(ReflectionProperty::IS_PUBLIC) as $property) {
            if ($property->isStatic() || in_array($property->getName(), self::IGNORED, true) || ! $property->isInitialized($event)) {
                continue;
            }

            $payload[$property->getName()] = $this->normalize($property->getValue($event), $event::class.'::$'.$property->getName());
        }

        return $payload;
    }

    public function normalize(mixed $value, string $path = 'payload'): mixed
    {
        return match (true) {
            $value === null, is_scalar($value) => $value,
            is_array($value) => array_map(fn ($item) => $this->normalize($item, $path.'[]'), $value),
            $value instanceof BackedEnum => $value->value,
            $value instanceof UnitEnum => $value->name,
            $value instanceof DateTimeInterface => $value->format(DateTimeInterface::RFC3339_EXTENDED),
            $value instanceof Model => ['type' => $value::class, 'id' => $value->getKey()],
            $value instanceof Arrayable => $this->normalize($value->toArray(), $path),
            $value instanceof JsonSerializable => $this->normalize($value->jsonSerialize(), $path),
            default => throw new SerializationException(sprintf(
                'Cannot put a value of type [%s] at %s into a Samsa payload. Implement %s on the event to control the payload.',
                get_debug_type($value),
                $path,
                ToSamsaPayload::class,
            )),
        };
    }
}
