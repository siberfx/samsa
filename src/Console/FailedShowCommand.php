<?php

namespace Siberfx\Samsa\Console;

use Illuminate\Console\Command;
use Siberfx\Samsa\Console\Concerns\UsesSamsaDriver;

class FailedShowCommand extends Command
{
    use UsesSamsaDriver;

    protected $signature = 'samsa:failed:show {id : The dead letter ID} {--driver= : The Samsa driver to use}';

    protected $description = 'Show the payload, headers and exception of a dead letter';

    public function handle(): int
    {
        $failed = $this->client()->deadLetters()->find((string) $this->argument('id'));

        if ($failed === null) {
            $this->components->error('No dead letter with that ID.');

            return self::FAILURE;
        }

        $record = $failed->record;

        $this->components->twoColumnDetail('ID', $failed->id);
        $this->components->twoColumnDetail('Group', $failed->group);
        $this->components->twoColumnDetail('Record', "{$record->topic}/{$record->partition}@{$record->offset}");
        $this->components->twoColumnDetail('Key', $record->key ?? '-');
        $this->components->twoColumnDetail('Event type', $record->eventType ?? '-');
        $this->components->twoColumnDetail('Produced at', $record->createdAt->toIso8601String());
        $this->components->twoColumnDetail('Failed at', $failed->failedAt->toIso8601String());
        $this->components->twoColumnDetail('Attempts', (string) $failed->attempts);

        $this->newLine();
        $this->line('<options=bold>Headers</>');
        $this->line((string) json_encode($record->headers, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE));
        $this->newLine();
        $this->line('<options=bold>Payload</>');
        $this->line((string) json_encode($record->payload, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE));
        $this->newLine();
        $this->line('<options=bold>Exception</>');
        $this->line($failed->exception);

        return self::SUCCESS;
    }
}
