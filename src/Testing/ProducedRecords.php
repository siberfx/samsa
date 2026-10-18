<?php

namespace Siberfx\Samsa\Testing;

use Siberfx\Samsa\Data\OutgoingRecord;
use Siberfx\Samsa\Data\RecordMetadata;

/**
 * @internal Records captured by SamsaFake, shared between its per-driver views.
 */
final class ProducedRecords
{
    /** @var array<string, list<OutgoingRecord>> */
    private array $records = [];

    public function add(string $topic, OutgoingRecord $record): RecordMetadata
    {
        $this->records[$topic][] = $record;

        return new RecordMetadata($topic, 0, (string) (count($this->records[$topic]) - 1));
    }

    /**
     * @return list<OutgoingRecord>
     */
    public function for(string $topic): array
    {
        return $this->records[$topic] ?? [];
    }

    /**
     * @return list<string>
     */
    public function topics(): array
    {
        return array_keys($this->records);
    }
}
