<?php

namespace Siberfx\Samsa\Console;

use Illuminate\Console\Command;
use Siberfx\Samsa\Console\Concerns\UsesSamsaDriver;
use Siberfx\Samsa\Data\TopicConfig;

class TopicCreateCommand extends Command
{
    use UsesSamsaDriver;

    protected $signature = 'samsa:topic:create
        {name? : The topic name}
        {--partitions= : Number of partitions (default: samsa.defaults.partitions)}
        {--from-config : Create or update every topic declared in samsa.topics}
        {--force : Allow growing the partition count of existing topics (with --from-config)}
        {--driver= : The Samsa driver to use}';

    protected $description = 'Create a topic, or every topic declared in config';

    public function handle(): int
    {
        $client = $this->client();

        if ($this->option('from-config')) {
            $declared = $client->topics()->declared();

            if ($declared === []) {
                $this->components->info('No topics are declared in samsa.topics.');

                return self::SUCCESS;
            }

            foreach ($declared as $topic) {
                $this->apply($topic);
            }

            return self::SUCCESS;
        }

        $name = $this->argument('name');

        if (! is_string($name) || $name === '') {
            $this->components->error('Pass a topic name, or use --from-config.');

            return self::FAILURE;
        }

        $partitions = $this->option('partitions') !== null ? (int) $this->option('partitions') : null;
        $this->apply($client->topics()->configured($name, $partitions));

        return self::SUCCESS;
    }

    private function apply(TopicConfig $wanted): void
    {
        $client = $this->client();
        $existing = $client->storage()->log()->topic($wanted->name);
        $result = $client->createTopic($wanted->name, $wanted->partitions, $wanted->retention);

        if ($existing === null) {
            $this->components->twoColumnDetail($wanted->name, "<fg=green>created</> ({$result->partitions} partitions)");

            return;
        }

        if ($wanted->partitions > $existing->partitions) {
            if ($this->option('force')) {
                $client->growTopic($wanted->name, $wanted->partitions);
                $this->components->twoColumnDetail($wanted->name, "<fg=yellow>grown</> {$existing->partitions} → {$wanted->partitions} partitions (keys remapped)");

                return;
            }

            $this->components->twoColumnDetail($wanted->name, "<fg=yellow>kept {$existing->partitions} partitions</> (config says {$wanted->partitions}; use --force or samsa:topic:alter)");

            return;
        }

        if ($wanted->partitions < $existing->partitions) {
            $this->components->twoColumnDetail($wanted->name, "<fg=yellow>kept {$existing->partitions} partitions</> (partition counts never shrink)");

            return;
        }

        $this->components->twoColumnDetail($wanted->name, 'up to date (retention updated)');
    }
}
