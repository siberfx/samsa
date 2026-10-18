<?php

namespace Siberfx\Samsa\Exceptions;

use Throwable;

class DriverException extends SamsaException
{
    public static function wrap(string $driver, Throwable $previous): self
    {
        return new self("The [{$driver}] driver failed: {$previous->getMessage()}", (int) $previous->getCode(), $previous);
    }
}
