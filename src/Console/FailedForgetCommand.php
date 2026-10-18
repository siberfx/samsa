<?php

namespace Siberfx\Samsa\Console;

use Illuminate\Console\Command;
use Siberfx\Samsa\Console\Concerns\UsesSamsaDriver;

class FailedForgetCommand extends Command
{
    use UsesSamsaDriver;

    protected $signature = 'samsa:failed:forget {id : The dead letter ID} {--driver= : The Samsa driver to use}';

    protected $description = 'Delete one dead letter';

    public function handle(): int
    {
        if (! $this->client()->deadLetters()->forget((string) $this->argument('id'))) {
            $this->components->error('No dead letter with that ID.');

            return self::FAILURE;
        }

        $this->components->info('Dead letter deleted.');

        return self::SUCCESS;
    }
}
