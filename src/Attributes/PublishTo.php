<?php

namespace Siberfx\Samsa\Attributes;

use Attribute;

/**
 * Produce this event to a topic whenever it is dispatched:
 *
 *     #[PublishTo('orders', key: 'orderId', eventType: 'order.placed')]
 *     final class OrderPlaced { ... }
 *
 * `key` names a public property or a zero-argument method of the event.
 */
#[Attribute(Attribute::TARGET_CLASS | Attribute::IS_REPEATABLE)]
final readonly class PublishTo
{
    public function __construct(
        public string $topic,
        public ?string $key = null,
        public ?string $eventType = null,
        public ?string $driver = null,
        public bool $afterCommit = true,
    ) {}
}
