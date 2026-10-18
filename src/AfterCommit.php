<?php

namespace Siberfx\Samsa;

use Illuminate\Database\DatabaseManager;

/**
 * Samsa::afterCommit()->produce(...): defers producing until the surrounding
 * transaction on the default connection commits (dropped on rollback). Without
 * an open transaction the record is produced immediately.
 */
final class AfterCommit
{
    public function __construct(
        private readonly Client $client,
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
        $produce = fn () => $this->client->produce($topic, $payload, $key, $headers, $dedupKey, $eventType);

        if ($this->db === null) {
            $produce();

            return;
        }

        $this->db->connection()->afterCommit($produce);
    }
}
