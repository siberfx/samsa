<?php

namespace Siberfx\Samsa\Exceptions;

class InvalidTopicException extends SamsaException
{
    public static function invalidName(string $name): self
    {
        return new self("Invalid topic name [{$name}]. Topic names must match [a-z0-9._-]+.");
    }

    public static function invalidPartitionCount(string $name, int $partitions): self
    {
        return new self("Invalid partition count [{$partitions}] for topic [{$name}]. Use 1 to 4096.");
    }

    public static function invalidPartition(string $name, int $partition): self
    {
        return new self("Topic [{$name}] has no partition [{$partition}].");
    }

    public static function cannotShrink(string $name, int $current, int $requested): self
    {
        return new self("Topic [{$name}] has {$current} partitions; partition counts can only grow (requested {$requested}).");
    }
}
