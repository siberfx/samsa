<?php

namespace Siberfx\Samsa\Console;

use Illuminate\Console\Command;
use Siberfx\Samsa\Console\Concerns\ParsesDurations;
use Siberfx\Samsa\Console\Concerns\UsesSamsaDriver;

class FailedFlushCommand extends Command
{
    use ParsesDurations;
    use UsesSamsaDriver;

    protected $signature = 'samsa:failed:flush
        {--older-than= : Only dead letters older than this, e.g. 30d}
        {--driver= : The Samsa driver to use}';

    protected $description = 'Delete dead letters in bulk';

    public function handle(): int
    {
        $before = $this->option('older-than') ? $this->ago((string) $this->option('older-than')) : null;
        $deleted = $this->client()->deadLetters()->flush($before);

        $this->components->info("{$deleted} dead letter(s) deleted.");

        return self::SUCCESS;
    }
}
