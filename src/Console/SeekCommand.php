<?php

namespace Siberfx\Samsa\Console;

use Illuminate\Console\Command;
use Siberfx\Samsa\Console\Concerns\UsesSamsaDriver;

class SeekCommand extends Command
{
    use UsesSamsaDriver;

    protected $signature = 'samsa:seek
        {group : The consumer group}
        {topic : The topic}
        {--to= : earliest, latest, offset:N, time:ISO-8601 or group:NAME}
        {--partition= : Only this partition (required for offset:N)}
        {--dry-run : Show the changes without applying them}
        {--driver= : The Samsa driver to use}';

    protected $description = 'Move the offsets of a consumer group (replay or skip)';

    public function handle(): int
    {
        $target = (string) $this->option('to');

        if ($target === '') {
            $this->components->error('Pass a target with --to=earliest|latest|offset:N|time:ISO-8601|group:NAME.');

            return self::FAILURE;
        }

        $offsets = $this->client()->offsets((string) $this->argument('group'), (string) $this->argument('topic'));
        $partition = $this->option('partition') !== null ? (int) $this->option('partition') : null;
        $plan = $offsets->plan($target, $partition);

        $this->table(['Partition', 'Committed now', 'After seek'], array_map(
            fn (int $p, ?string $to) => [$p, $offsets->committed($p) ?? 'beginning', $to ?? 'beginning'],
            array_keys($plan),
            array_values($plan),
        ));

        if ($this->option('dry-run')) {
            $this->components->info('Dry run: nothing changed.');

            return self::SUCCESS;
        }

        $offsets->apply($plan);
        $this->components->info('Offsets moved. Running workers pick this up on their next rebalance.');

        return self::SUCCESS;
    }
}
