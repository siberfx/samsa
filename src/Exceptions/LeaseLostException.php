<?php

namespace Siberfx\Samsa\Exceptions;

class LeaseLostException extends SamsaException
{
    public static function for(string $group, string $topic, int $partition, string $memberId): self
    {
        return new self("Member [{$memberId}] no longer holds the lease on [{$topic}] partition {$partition} for group [{$group}].");
    }
}
