<?php

namespace Siberfx\Samsa\Console;

use Illuminate\Console\Command;
use Illuminate\Support\Sleep;
use Siberfx\Samsa\Console\Concerns\UsesSamsaDriver;
use Siberfx\Samsa\Exceptions\TopicNotFoundException;

class TailCommand extends Command
{
    use UsesSamsaDriver;

    protected $signature = 'samsa:tail
        {topic : The topic}
        {--partition= : Only this partition}
        {--from=latest : latest or earliest}
        {--limit= : Exit after printing N records}
        {--driver= : The Samsa driver to use}';

    protected $description = 'Print records as they arrive, without committing anything';

    public function handle(): int
    {
        $topic = (string) $this->argument('topic');
        $log = $this->client()->storage()->log();
        $config = $log->topic($topic) ?? throw TopicNotFoundException::named($topic);

        $partitions = $this->option('partition') !== null
            ? [(int) $this->option('partition')]
            : range(0, $config->partitions - 1);

        $positions = [];

        foreach ($partitions as $p) {
            $positions[$p] = $this->option('from') === 'earliest' ? null : $log->latestOffset($topic, $p);
        }

        $limit = $this->option('limit') !== null ? (int) $this->option('limit') : null;
        $printed = 0;

        while (true) {
            $any = false;

            foreach ($partitions as $p) {
                foreach ($log->read($topic, $p, $positions[$p], 100) as $record) {
                    $any = true;
                    $positions[$p] = $record->offset;

                    $this->line(sprintf(
                        '<fg=gray>%s</> <fg=cyan>%s/%d@%s</> key=<fg=yellow>%s</>%s %s',
                        $record->createdAt->format('H:i:s.v'),
                        $topic,
                        $p,
                        $record->offset,
                        $record->key ?? '-',
                        $record->eventType !== null ? " type={$record->eventType}" : '',
                        json_encode($record->payload, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES),
                    ));

                    if ($limit !== null && ++$printed >= $limit) {
                        return self::SUCCESS;
                    }
                }
            }

            if (! $any) {
                if ($limit !== null && $this->option('from') === 'earliest') {
                    return self::SUCCESS;
                }

                Sleep::for(250)->milliseconds();
            }
        }
    }
}
