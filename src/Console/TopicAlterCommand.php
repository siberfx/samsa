<?php

namespace Siberfx\Samsa\Console;

use Illuminate\Console\Command;
use Siberfx\Samsa\Console\Concerns\UsesSamsaDriver;

class TopicAlterCommand extends Command
{
    use UsesSamsaDriver;

    protected $signature = 'samsa:topic:alter
        {name : The topic name}
        {--partitions= : The new (larger) partition count}
        {--force : Confirm that keys will be remapped}
        {--driver= : The Samsa driver to use}';

    protected $description = 'Increase the partition count of a topic';

    public function handle(): int
    {
        if ($this->option('partitions') === null) {
            $this->components->error('Pass the new partition count with --partitions=N.');

            return self::FAILURE;
        }

        if (! $this->option('force')) {
            $this->components->warn('Growing a topic remaps keys to partitions: per-key ordering is not guaranteed while older records drain.');
            $this->components->error('Re-run with --force to continue.');

            return self::FAILURE;
        }

        $config = $this->client()->growTopic((string) $this->argument('name'), (int) $this->option('partitions'));
        $this->components->info("Topic [{$config->name}] now has {$config->partitions} partitions.");

        return self::SUCCESS;
    }
}
