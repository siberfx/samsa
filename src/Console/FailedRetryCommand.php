<?php

namespace Siberfx\Samsa\Console;

use Closure;
use Illuminate\Console\Command;
use Siberfx\Samsa\Console\Concerns\UsesSamsaDriver;
use Siberfx\Samsa\Contracts\BatchHandler;
use Siberfx\Samsa\Data\FailedRecord;
use Throwable;

class FailedRetryCommand extends Command
{
    use UsesSamsaDriver;

    protected $signature = 'samsa:failed:retry
        {id? : The dead letter ID}
        {--all : Retry every dead letter (optionally filtered by --group / --topic)}
        {--group= : Only dead letters of this group}
        {--topic= : Only dead letters of this topic}
        {--in-place : Run the group handler now instead of re-producing the record}
        {--driver= : The Samsa driver to use}';

    protected $description = 'Re-produce dead letters to their topic, or re-handle them in place';

    public function handle(): int
    {
        $client = $this->client();
        $store = $client->deadLetters();

        $failed = match (true) {
            is_string($this->argument('id')) => array_filter([$store->find((string) $this->argument('id'))]),
            (bool) $this->option('all') => $store->all($this->option('group') ?: null, $this->option('topic') ?: null, PHP_INT_MAX),
            default => null,
        };

        if ($failed === null) {
            $this->components->error('Pass a dead letter ID or --all.');

            return self::FAILURE;
        }

        if ($failed === []) {
            $this->components->info('Nothing to retry.');

            return self::SUCCESS;
        }

        $failures = 0;

        foreach ($failed as $item) {
            if ($this->option('in-place')) {
                $failures += $this->retryInPlace($item) ? 0 : 1;

                continue;
            }

            $meta = $client->retryDeadLetter($item->id);
            $this->components->twoColumnDetail($item->id, "re-produced to {$meta?->topic}/{$meta?->partition}@{$meta?->offset}");
        }

        return $failures === 0 ? self::SUCCESS : self::FAILURE;
    }

    private function retryInPlace(FailedRecord $failed): bool
    {
        $handler = $this->samsa()->subscriptions()->handler($failed->group, $failed->record->topic);

        if ($handler === null) {
            $this->components->twoColumnDetail($failed->id, "<fg=red>no handler subscribed for [{$failed->group}] on [{$failed->record->topic}]</>");

            return false;
        }

        $handler = is_string($handler) ? $this->laravel->make($handler) : $handler;
        $record = $failed->record->withDeliveryAttempt($failed->attempts + 1);

        try {
            match (true) {
                $handler instanceof Closure => $handler($record),
                $handler instanceof BatchHandler => $handler->handleBatch([$record]),
                default => $handler->handle($record),
            };
        } catch (Throwable $e) {
            $this->components->twoColumnDetail($failed->id, "<fg=red>failed again: {$e->getMessage()}</>");

            return false;
        }

        $this->client()->deadLetters()->forget($failed->id);
        $this->components->twoColumnDetail($failed->id, '<fg=green>handled</>');

        return true;
    }
}
