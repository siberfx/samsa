<?php

namespace Siberfx\Samsa\Console;

use Illuminate\Console\Command;
use Siberfx\Samsa\Console\Concerns\UsesSamsaDriver;

class ReleaseDelayedCommand extends Command
{
    use UsesSamsaDriver;

    protected $signature = 'samsa:release-delayed
        {--limit=500 : Maximum records to release per run}
        {--driver= : The Samsa driver to use}';

    protected $description = 'Move due delayed records (Queue::later, released jobs, DelayStamps) into their topics';

    public function handle(): int
    {
        $released = $this->client()->releaseDelayed((int) $this->option('limit'));

        if ($released > 0 || $this->getOutput()->isVerbose()) {
            $this->components->info("{$released} delayed record(s) released.");
        }

        return self::SUCCESS;
    }
}
