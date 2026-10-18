<?php

namespace Siberfx\Samsa\Idempotency;

use Carbon\CarbonImmutable;
use Closure;
use DateTimeInterface;
use Illuminate\Database\Connection;
use Illuminate\Database\DatabaseManager;
use Illuminate\Database\UniqueConstraintViolationException;
use InvalidArgumentException;
use Siberfx\Samsa\Data\Record;
use Siberfx\Samsa\Drivers\Database\DatabaseClock;
use Siberfx\Samsa\Drivers\Database\Tables;

/**
 * Run a side effect at most once per (scope, message_id), atomically with
 * whatever the callback writes to the same database.
 */
final class OnlyOnce
{
    private readonly Tables $tables;

    public function __construct(
        private readonly DatabaseManager $db,
        private readonly ?string $connection = null,
        string $tablePrefix = '',
    ) {
        $this->tables = new Tables($tablePrefix);
    }

    /**
     * Returns true when the callback ran, false when the message was already processed.
     */
    public function run(string $scope, Record $record, Closure $callback): bool
    {
        $messageId = $record->messageId() ?? throw new InvalidArgumentException('The record has no message_id header.');

        return $this->connection()->transaction(function (Connection $connection) use ($scope, $messageId, $callback) {
            try {
                // A savepoint keeps PostgreSQL's transaction usable after a duplicate key.
                $connection->transaction(fn () => $connection->table($this->tables->processed)->insert([
                    'scope' => $scope,
                    'message_id' => $messageId,
                    'processed_at' => DatabaseClock::format(CarbonImmutable::now('UTC')),
                ]));
            } catch (UniqueConstraintViolationException) {
                return false;
            }

            $callback();

            return true;
        });
    }

    public function prune(DateTimeInterface $before): int
    {
        return $this->connection()->table($this->tables->processed)
            ->where('processed_at', '<', DatabaseClock::format(CarbonImmutable::instance($before)))
            ->delete();
    }

    private function connection(): Connection
    {
        /** @var Connection */
        return $this->db->connection($this->connection);
    }
}
