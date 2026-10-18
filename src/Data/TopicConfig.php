<?php

namespace Siberfx\Samsa\Data;

use Siberfx\Samsa\Exceptions\InvalidTopicException;

final readonly class TopicConfig
{
    public RetentionPolicy $retention;

    public function __construct(
        public string $name,
        public int $partitions = 8,
        ?RetentionPolicy $retention = null,
    ) {
        if (preg_match('/^[a-z0-9._-]+$/', $name) !== 1) {
            throw InvalidTopicException::invalidName($name);
        }

        if ($partitions < 1 || $partitions > 4096) {
            throw InvalidTopicException::invalidPartitionCount($name, $partitions);
        }

        $this->retention = $retention ?? new RetentionPolicy;
    }
}
