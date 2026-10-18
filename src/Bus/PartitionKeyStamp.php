<?php

namespace Siberfx\Samsa\Bus;

use Siberfx\MessageBus\Stamp\StampInterface;

/**
 * Set the Samsa partition key for one dispatch:
 *
 *     MessageBus::dispatch($message, [new PartitionKeyStamp("order-{$id}")]);
 */
final readonly class PartitionKeyStamp implements StampInterface
{
    public function __construct(public string $key) {}
}
