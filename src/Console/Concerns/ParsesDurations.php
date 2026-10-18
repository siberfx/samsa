<?php

namespace Siberfx\Samsa\Console\Concerns;

use Carbon\CarbonImmutable;
use InvalidArgumentException;

trait ParsesDurations
{
    /**
     * "30d", "12h", "15m", "45s" → the moment that long ago.
     */
    protected function ago(string $duration): CarbonImmutable
    {
        if (preg_match('/^(\d+)\s*([smhdw])$/i', trim($duration), $m) !== 1) {
            throw new InvalidArgumentException("Invalid duration [{$duration}]. Use e.g. 45s, 15m, 12h, 30d or 2w.");
        }

        $units = ['s' => 1, 'm' => 60, 'h' => 3600, 'd' => 86400, 'w' => 604800];
        $seconds = (int) $m[1] * $units[strtolower($m[2])];

        return CarbonImmutable::now('UTC')->subSeconds($seconds);
    }
}
