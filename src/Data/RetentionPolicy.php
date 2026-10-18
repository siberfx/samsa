<?php

namespace Siberfx\Samsa\Data;

final readonly class RetentionPolicy
{
    public function __construct(
        public ?int $maxAgeSeconds = null,
        public ?int $maxRecordsPerPartition = null,
        public bool $respectSlowestConsumer = false,
    ) {}

    /**
     * @param  array{max_age_seconds?: ?int, max_records_per_partition?: ?int, respect_slowest_consumer?: bool}  $config
     */
    public static function fromArray(array $config): self
    {
        return new self(
            isset($config['max_age_seconds']) ? (int) $config['max_age_seconds'] : null,
            isset($config['max_records_per_partition']) ? (int) $config['max_records_per_partition'] : null,
            (bool) ($config['respect_slowest_consumer'] ?? false),
        );
    }

    /**
     * @return array{max_age_seconds: ?int, max_records_per_partition: ?int, respect_slowest_consumer: bool}
     */
    public function toArray(): array
    {
        return [
            'max_age_seconds' => $this->maxAgeSeconds,
            'max_records_per_partition' => $this->maxRecordsPerPartition,
            'respect_slowest_consumer' => $this->respectSlowestConsumer,
        ];
    }
}
