<?php

namespace Siberfx\Samsa\Console;

use Illuminate\Console\Command;
use Siberfx\Samsa\Console\Concerns\UsesSamsaDriver;
use Siberfx\Samsa\Exceptions\TopicNotFoundException;

class TopicDeleteCommand extends Command
{
    use UsesSamsaDriver;

    protected $signature = 'samsa:topic:delete
        {name : The topic name}
        {--force : Confirm deleting the topic, its records and every group position on it}
        {--driver= : The Samsa driver to use}';

    protected $description = 'Delete a topic, its records and all group offsets for it';

    public function handle(): int
    {
        $name = (string) $this->argument('name');
        $client = $this->client();

        if ($client->storage()->log()->topic($name) === null) {
            throw TopicNotFoundException::named($name);
        }

        if (! $this->option('force')) {
            $this->components->error("This permanently deletes [{$name}] and every record in it. Re-run with --force.");

            return self::FAILURE;
        }

        $client->deleteTopic($name);
        $this->components->info("Topic [{$name}] deleted.");

        return self::SUCCESS;
    }
}
