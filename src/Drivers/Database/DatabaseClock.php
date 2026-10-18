<?php

namespace Siberfx\Samsa\Drivers\Database;

use Carbon\CarbonImmutable;
use Illuminate\Database\Connection;
use Throwable;

/**
 * "Now" according to the database server, so that leases don't depend on the
 * clocks of individual worker machines. The skew between PHP and the database
 * is measured once a minute; with Carbon's test clock active, PHP time is used.
 */
final class DatabaseClock
{
    private ?float $skewSeconds = null;

    private float $measuredAt = 0.0;

    public function __construct(private readonly Connection $connection) {}

    public function now(): CarbonImmutable
    {
        $now = CarbonImmutable::now('UTC');

        if (CarbonImmutable::hasTestNow()) {
            return $now;
        }

        if ($this->skewSeconds === null || microtime(true) - $this->measuredAt > 60) {
            $this->skewSeconds = $this->measureSkew();
            $this->measuredAt = microtime(true);
        }

        return $now->addMicroseconds((int) round($this->skewSeconds * 1_000_000));
    }

    public static function format(CarbonImmutable $time): string
    {
        return $time->setTimezone('UTC')->format('Y-m-d H:i:s.u');
    }

    public static function parse(?string $value): ?CarbonImmutable
    {
        return $value === null ? null : CarbonImmutable::parse($value, 'UTC');
    }

    private function measureSkew(): float
    {
        $sql = match ($this->connection->getDriverName()) {
            'mysql', 'mariadb' => 'select UTC_TIMESTAMP(6) as now',
            'pgsql' => "select to_char(now() at time zone 'utc', 'YYYY-MM-DD HH24:MI:SS.US') as now",
            'sqlite' => "select strftime('%Y-%m-%d %H:%M:%f', 'now') as now",
            default => null,
        };

        if ($sql === null) {
            return 0.0;
        }

        try {
            $before = microtime(true);
            $row = $this->connection->selectOne($sql);
            $after = microtime(true);
        } catch (Throwable) {
            return 0.0;
        }

        $databaseNow = CarbonImmutable::parse((string) $row->now, 'UTC');
        $phpNow = CarbonImmutable::createFromTimestampUTC(($before + $after) / 2);

        return ($databaseNow->getTimestamp() + $databaseNow->micro / 1_000_000)
            - ($phpNow->getTimestamp() + $phpNow->micro / 1_000_000);
    }
}
