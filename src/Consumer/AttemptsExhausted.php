<?php

namespace Siberfx\Samsa\Consumer;

use Siberfx\Samsa\Exceptions\SamsaException;
use Throwable;

/**
 * Wraps a handler's final exception together with the number of attempts made.
 */
final class AttemptsExhausted extends SamsaException
{
    public int $attempts = 1;

    public static function wrap(Throwable $previous, int $attempts): self
    {
        $e = new self("Record failed after {$attempts} attempt(s): {$previous->getMessage()}", 0, $previous);
        $e->attempts = $attempts;

        return $e;
    }
}
