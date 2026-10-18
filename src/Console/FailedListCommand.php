<?php

namespace Siberfx\Samsa\Console;

use Illuminate\Console\Command;
use Illuminate\Support\Str;
use Siberfx\Samsa\Console\Concerns\UsesSamsaDriver;
use Siberfx\Samsa\Data\FailedRecord;

class FailedListCommand extends Command
{
    use UsesSamsaDriver;

    protected $signature = 'samsa:failed
        {--group= : Only this group}
        {--topic= : Only this topic}
        {--limit=100 : Maximum number of rows}
        {--driver= : The Samsa driver to use}';

    protected $description = 'List dead-lettered records';

    public function handle(): int
    {
        $failed = $this->client()->deadLetters()->all(
            $this->option('group') ?: null,
            $this->option('topic') ?: null,
            (int) $this->option('limit'),
        );

        if ($failed === []) {
            $this->components->info('No dead letters.');

            return self::SUCCESS;
        }

        $this->table(['ID', 'Group', 'Record', 'Key', 'Attempts', 'Failed at', 'Error'], array_map(fn (FailedRecord $f) => [
            $f->id,
            $f->group,
            "{$f->record->topic}/{$f->record->partition}@{$f->record->offset}",
            $f->record->key ?? '-',
            $f->attempts,
            $f->failedAt->toDateTimeString(),
            Str::limit(strtok($f->exception, "\n") ?: '', 60),
        ], $failed));

        return self::SUCCESS;
    }
}
