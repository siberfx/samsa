<?php

namespace Siberfx\Samsa\Console;

use Illuminate\Console\Command;
use Siberfx\Samsa\Console\Concerns\UsesSamsaDriver;

class GroupsCommand extends Command
{
    use UsesSamsaDriver;

    protected $signature = 'samsa:groups {--driver= : The Samsa driver to use}';

    protected $description = 'List consumer groups with their topics, members and total lag';

    public function handle(): int
    {
        $client = $this->client();
        $rows = [];

        foreach ($client->storage()->offsets()->allGroups() as $group => $topics) {
            foreach ($topics as $topic) {
                if ($client->storage()->log()->topic($topic) === null) {
                    continue;
                }

                $rows[] = [
                    $group,
                    $topic,
                    count($client->storage()->leases()->members($group, $topic)),
                    $client->stats()->lag($group, $topic),
                ];
            }
        }

        if ($rows === []) {
            $this->components->info('No consumer groups yet.');

            return self::SUCCESS;
        }

        $this->table(['Group', 'Topic', 'Members', 'Lag'], $rows);

        return self::SUCCESS;
    }
}
