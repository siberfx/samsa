<?php

namespace Siberfx\Samsa\Queue;

use Illuminate\Container\Container;
use Illuminate\Contracts\Queue\Job as JobContract;
use Illuminate\Queue\Jobs\Job;
use Siberfx\Samsa\Data\Record;

/**
 * A Laravel job read from a Samsa topic. delete() commits its offset;
 * release() re-produces it (optionally delayed) and commits; fail() deletes.
 */
class SamsaJob extends Job implements JobContract
{
    public function __construct(
        Container $container,
        private readonly SamsaQueue $samsa,
        private readonly Record $record,
        string $connectionName,
        string $queue,
    ) {
        $this->container = $container;
        $this->connectionName = $connectionName;
        $this->queue = $queue;
    }

    public function getJobId(): ?string
    {
        return $this->payload()['uuid'] ?? $this->record->messageId();
    }

    public function getRawBody(): string
    {
        return (string) ($this->record->payload['job'] ?? '');
    }

    public function attempts(): int
    {
        return (int) ($this->record->payload['attempts'] ?? 0) + 1;
    }

    public function delete(): void
    {
        if ($this->isDeleted()) {
            return;
        }

        parent::delete();

        if (! $this->isReleased()) {
            $this->samsa->acknowledge($this);
        }
    }

    public function release($delay = 0): void
    {
        parent::release($delay);

        $this->samsa->releaseJob($this, (int) $delay);
    }

    public function record(): Record
    {
        return $this->record;
    }
}
