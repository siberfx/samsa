<?php

namespace Siberfx\Samsa\Producer;

use Carbon\CarbonImmutable;
use Illuminate\Contracts\Events\Dispatcher;
use Illuminate\Support\Str;
use Siberfx\Samsa\Contracts\LogStore;
use Siberfx\Samsa\Contracts\Partitioner;
use Siberfx\Samsa\Data\OutgoingRecord;
use Siberfx\Samsa\Data\RecordMetadata;
use Siberfx\Samsa\Events\RecordProduced;
use Siberfx\Samsa\Exceptions\SerializationException;

final class Producer
{
    public function __construct(
        private readonly LogStore $log,
        private readonly TopicRegistry $topics,
        private readonly Partitioner $partitioner,
        private readonly ?Dispatcher $events = null,
        private readonly string $producerName = 'laravel',
    ) {}

    public function produce(string $topic, OutgoingRecord $record): RecordMetadata
    {
        $config = $this->topics->resolve($topic);
        $record = $this->prepare($record);

        $metadata = $this->log->append($topic, $this->partitioner->partition($record->key, $config->partitions), $record);

        $this->events?->dispatch(new RecordProduced($metadata, $record));

        return $metadata;
    }

    /**
     * Records are grouped by partition; order is preserved within each key.
     *
     * @param  list<OutgoingRecord>  $records
     * @return list<RecordMetadata> in the same order as $records
     */
    public function produceBatch(string $topic, array $records): array
    {
        $config = $this->topics->resolve($topic);
        $byPartition = [];

        foreach (array_values($records) as $index => $record) {
            $record = $this->prepare($record);
            $byPartition[$this->partitioner->partition($record->key, $config->partitions)][$index] = $record;
        }

        $metadata = [];

        foreach ($byPartition as $partition => $partitionRecords) {
            $results = $this->log->appendBatch($topic, $partition, array_values($partitionRecords));

            foreach (array_keys($partitionRecords) as $i => $index) {
                $metadata[$index] = $results[$i];
                $this->events?->dispatch(new RecordProduced($results[$i], $partitionRecords[$index]));
            }
        }

        ksort($metadata);

        return array_values($metadata);
    }

    private function prepare(OutgoingRecord $record): OutgoingRecord
    {
        foreach ($record->headers as $name => $value) {
            if (! is_string($name) || ($value !== null && ! is_scalar($value))) {
                throw new SerializationException('Header values must be scalars or null, keyed by string names.');
            }
        }

        return new OutgoingRecord(
            key: $record->key,
            payload: $record->payload,
            headers: [
                'message_id' => (string) Str::ulid(),
                'produced_at' => CarbonImmutable::now('UTC')->format('Y-m-d\TH:i:s.uP'),
                'producer' => $this->producerName,
                ...$record->headers,
            ],
            dedupKey: $record->dedupKey,
            eventType: $record->eventType,
        );
    }
}
