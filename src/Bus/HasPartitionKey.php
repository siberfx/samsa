<?php

namespace Siberfx\Samsa\Bus;

/**
 * Implement on a message-bus message to keep messages for one entity in order.
 */
interface HasPartitionKey
{
    public function partitionKey(): string;
}
