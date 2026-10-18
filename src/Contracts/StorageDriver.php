<?php

namespace Siberfx\Samsa\Contracts;

/**
 * A storage backend. Every driver must pass the StorageDriverContractTest suite.
 */
interface StorageDriver
{
    public function log(): LogStore;

    public function offsets(): OffsetStore;

    public function leases(): LeaseStore;

    public function deadLetters(): DeadLetterStore;
}
