<?php

namespace Siberfx\Samsa\Drivers\InMemory;

use Carbon\CarbonImmutable;
use Closure;
use DateTimeInterface;
use Siberfx\Samsa\Contracts\DelayedStore;
use Siberfx\Samsa\Data\OutgoingRecord;

final class InMemoryDelayedStore implements DelayedStore
{
    /** @var array<int, array{topic: string, record: OutgoingRecord, at: CarbonImmutable}> */
    private array $items = [];

    private int $nextId = 1;

    public function schedule(string $topic, OutgoingRecord $record, DateTimeInterface $availableAt): string
    {
        $id = $this->nextId++;
        $this->items[$id] = ['topic' => $topic, 'record' => $record, 'at' => CarbonImmutable::instance($availableAt)];

        return (string) $id;
    }

    public function release(Closure $produce, int $limit = 500): int
    {
        $now = CarbonImmutable::now();
        $due = array_filter($this->items, fn (array $item) => $item['at']->lessThanOrEqualTo($now));
        uasort($due, fn (array $a, array $b) => $a['at'] <=> $b['at']);

        $released = 0;

        foreach (array_slice($due, 0, $limit, true) as $id => $item) {
            $produce($item['topic'], $item['record']);
            unset($this->items[$id]);
            $released++;
        }

        return $released;
    }

    public function count(?string $topic = null): int
    {
        return count(array_filter($this->items, fn (array $item) => $topic === null || $item['topic'] === $topic));
    }
}
