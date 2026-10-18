<?php

namespace Siberfx\Samsa\Console;

use Illuminate\Console\Command;
use Siberfx\Samsa\Console\Concerns\UsesSamsaDriver;

class TopicListCommand extends Command
{
    use UsesSamsaDriver;

    protected $signature = 'samsa:topic:list {--driver= : The Samsa driver to use}';

    protected $description = 'List topics with partition counts, record counts and retention';

    public function handle(): int
    {
        $log = $this->client()->storage()->log();
        $topics = $log->topics();

        if ($topics === []) {
            $this->components->info('No topics yet. Create one with `php artisan samsa:topic:create {name}`.');

            return self::SUCCESS;
        }

        $this->table(['Topic', 'Partitions', 'Records', 'Retention'], array_map(function ($topic) use ($log) {
            $records = 0;

            for ($p = 0; $p < $topic->partitions; $p++) {
                $records += $log->countAfter($topic->name, $p, null);
            }

            $retention = array_filter([
                $topic->retention->maxAgeSeconds !== null ? $this->humanSeconds($topic->retention->maxAgeSeconds) : null,
                $topic->retention->maxRecordsPerPartition !== null ? "{$topic->retention->maxRecordsPerPartition}/partition" : null,
                $topic->retention->respectSlowestConsumer ? 'respects slowest group' : null,
            ]);

            return [$topic->name, $topic->partitions, $records, $retention === [] ? 'forever' : implode(', ', $retention)];
        }, $topics));

        return self::SUCCESS;
    }

    private function humanSeconds(int $seconds): string
    {
        return match (true) {
            $seconds % 86400 === 0 => ($seconds / 86400).'d',
            $seconds % 3600 === 0 => ($seconds / 3600).'h',
            $seconds % 60 === 0 => ($seconds / 60).'m',
            default => $seconds.'s',
        };
    }
}
