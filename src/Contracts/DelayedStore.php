<?php

namespace Siberfx\Samsa\Contracts;

use Closure;
use DateTimeInterface;
use Siberfx\Samsa\Data\OutgoingRecord;

/**
 * Holds records that must not enter the log before a given time
 * (Queue::later(), $job->release($delay), message-bus DelayStamp).
 * A log can't hold a record back, so they wait here until released.
 */
interface DelayedStore
{
    /**
     * @return string an identifier for the delayed record
     */
    public function schedule(string $topic, OutgoingRecord $record, DateTimeInterface $availableAt): string;

    /**
     * Hand every due record to $produce (oldest first) and remove it.
     * Returns the number of released records.
     *
     * @param  Closure(string $topic, OutgoingRecord $record): mixed  $produce
     */
    public function release(Closure $produce, int $limit = 500): int;

    public function count(?string $topic = null): int;
}
