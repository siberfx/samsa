<?php

namespace Siberfx\Samsa\Console;

use Illuminate\Console\Command;
use Illuminate\Contracts\Cache\Repository;
use Siberfx\Samsa\Consumer\Worker;

class RestartCommand extends Command
{
    protected $signature = 'samsa:restart';

    protected $description = 'Ask every Samsa worker to exit gracefully after its current batch';

    public function handle(Repository $cache): int
    {
        $cache->forever(Worker::RESTART_CACHE_KEY, (string) microtime(true));

        $this->components->info('Broadcasting Samsa restart signal.');

        return self::SUCCESS;
    }
}
