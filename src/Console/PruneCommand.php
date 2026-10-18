<?php

namespace Siberfx\Samsa\Console;

use Illuminate\Console\Command;
use Siberfx\Samsa\Console\Concerns\ParsesDurations;
use Siberfx\Samsa\Console\Concerns\UsesSamsaDriver;
use Siberfx\Samsa\Data\TopicConfig;
use Siberfx\Samsa\Idempotency\OnlyOnce;

class PruneCommand extends Command
{
    use ParsesDurations;
    use UsesSamsaDriver;

    protected $signature = 'samsa:prune
        {--topic= : Only this topic}
        {--dry-run : Show the retention that would be applied}
        {--processed-older-than= : Also delete OnlyOnce markers older than this, e.g. 30d}
        {--driver= : The Samsa driver to use}';

    protected $description = 'Delete records according to each topic retention policy';

    public function handle(): int
    {
        $client = $this->client();
        $storage = $client->storage();

        $topics = array_filter(
            $storage->log()->topics(),
            fn (TopicConfig $t) => ! $this->option('topic') || $t->name === $this->option('topic'),
        );

        foreach ($topics as $topic) {
            $policy = $topic->retention;

            if ($this->option('dry-run')) {
                $this->components->twoColumnDetail($topic->name, sprintf(
                    'max age: %s, max per partition: %s%s',
                    $policy->maxAgeSeconds === null ? 'none' : $policy->maxAgeSeconds.'s',
                    $policy->maxRecordsPerPartition ?? 'none',
                    $policy->respectSlowestConsumer ? ', respecting the slowest group' : '',
                ));

                continue;
            }

            $protect = null;

            if ($policy->respectSlowestConsumer) {
                $protect = [];

                foreach (range(0, $topic->partitions - 1) as $partition) {
                    $slowest = PHP_INT_MAX;

                    foreach ($storage->offsets()->groups($topic->name) as $group) {
                        $committed = $storage->offsets()->committed($group, $topic->name, $partition);
                        $slowest = min($slowest, $committed === null ? -1 : (int) $committed);
                    }

                    $protect[$partition] = match (true) {
                        $slowest === PHP_INT_MAX => $storage->log()->latestOffset($topic->name, $partition),
                        $slowest < 0 => null,
                        default => (string) $slowest,
                    };
                }
            }

            $deleted = $storage->log()->prune($topic->name, $policy, $protect);
            $this->components->twoColumnDetail($topic->name, "{$deleted} record(s) deleted");
        }

        if ($this->option('processed-older-than') && ! $this->option('dry-run')) {
            $deleted = $this->laravel->make(OnlyOnce::class)->prune($this->ago((string) $this->option('processed-older-than')));
            $this->components->twoColumnDetail('OnlyOnce markers', "{$deleted} deleted");
        }

        return self::SUCCESS;
    }
}
