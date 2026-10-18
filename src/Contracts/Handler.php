<?php

namespace Siberfx\Samsa\Contracts;

use Siberfx\Samsa\Data\Record;

interface Handler
{
    /**
     * Returning normally commits the record; throwing triggers a retry.
     */
    public function handle(Record $record): void;
}
