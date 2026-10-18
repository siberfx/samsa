<?php

namespace Siberfx\Samsa;

use Closure;
use Illuminate\Database\DatabaseManager;

/**
 * Samsa::afterCommit()->produce(...): defers producing until the surrounding
 * transaction on the default connection commits (dropped on rollback). Without
 * an open transaction the record is produced immediately.
 */
final class AfterCommit
{
    /**
     * @param  Closure(string, array<string, mixed>, ?string, array<string, scalar|null>, ?string, ?string): mixed  $produce
     */
    public function __construct(
        private readonly Closure $produce,
        private readonly ?DatabaseManager $db,
    ) {}

    /**
     * @param  array<string, mixed>  $payload
     * @param  array<string, scalar|null>  $headers
     */
    public function produce(
        string $topic,
        array $payload,
        string|int|null $key = null,
        array $headers = [],
        ?string $dedupKey = null,
        ?string $eventType = null,
    ): void {
        $produce = fn () => ($this->produce)($topic, $payload, $key === null ? null : (string) $key, $headers, $dedupKey, $eventType);

        if ($this->db === null) {
            $produce();

            return;
        }

        $this->db->connection()->afterCommit($produce);
    }
}
