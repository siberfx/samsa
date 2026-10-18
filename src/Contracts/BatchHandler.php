<?php

namespace Siberfx\Samsa\Contracts;

use Siberfx\Samsa\Data\Record;

interface BatchHandler
{
    /**
     * @param  list<Record>  $records  all from the same partition, in order
     */
    public function handleBatch(array $records): void;
}
