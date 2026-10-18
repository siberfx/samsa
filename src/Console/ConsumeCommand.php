<?php

namespace Siberfx\Samsa\Console;

use Illuminate\Console\Command;
use Siberfx\Samsa\Console\Concerns\UsesSamsaDriver;
use Siberfx\Samsa\Consumer\WorkerOptions;

class ConsumeCommand extends Command
{
    use UsesSamsaDriver;

    protected $signature = 'samsa:consume
        {group : The consumer group}
        {topic* : One or more topics}
        {--batch= : Records per read per partition}
        {--sleep= : Idle sleep in milliseconds}
        {--commit= : batch or record}
        {--partitions= : Only claim these partitions, e.g. 0,1,2 (disables fair share)}
        {--once : Process at most one batch per partition, then exit}
        {--stop-when-empty : Exit when every owned partition is caught up}
        {--max-records= : Exit after N records}
        {--max-time= : Exit after N seconds}
        {--memory=128 : Exit gracefully above N MB}
        {--driver= : The Samsa driver to use}';

    protected $description = 'Consume records for a consumer group';

    public function handle(): int
    {
        $partitions = $this->option('partitions');

        $options = WorkerOptions::fromConfig((array) config('samsa.consumer', []), [
            'batchSize' => $this->intOption('batch'),
            'sleepMs' => $this->intOption('sleep'),
            'commit' => $this->option('commit') ?: null,
            'partitions' => is_string($partitions) && $partitions !== ''
                ? array_map('intval', explode(',', $partitions))
                : null,
            'once' => (bool) $this->option('once'),
            'stopWhenEmpty' => (bool) $this->option('stop-when-empty'),
            'maxRecords' => $this->intOption('max-records'),
            'maxTime' => $this->intOption('max-time'),
            'memoryMb' => $this->intOption('memory'),
        ]);

        $worker = $this->samsa()->worker($this->driverName());

        $worker->setOutput(function (string $level, string $message) {
            if ($level === 'warn') {
                $this->components->warn($message);
            } elseif ($this->getOutput()->isVerbose()) {
                $this->components->info($message);
            }
        });

        return $worker->run((string) $this->argument('group'), array_values((array) $this->argument('topic')), $options);
    }

    private function intOption(string $name): ?int
    {
        $value = $this->option($name);

        return $value === null || $value === '' ? null : (int) $value;
    }
}
