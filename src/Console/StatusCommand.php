<?php

namespace Siberfx\Samsa\Console;

use Illuminate\Console\Command;
use Siberfx\Samsa\Console\Concerns\UsesSamsaDriver;

class StatusCommand extends Command
{
    use UsesSamsaDriver;

    protected $signature = 'samsa:status
        {--group= : Only this group}
        {--topic= : Only this topic}
        {--partition= : Only this partition}
        {--json : Machine-readable output}
        {--driver= : The Samsa driver to use}';

    protected $description = 'Show offsets, lag and partition owners per consumer group';

    public function handle(): int
    {
        $client = $this->client();
        $groups = $client->storage()->offsets()->allGroups();

        // Subscribed groups appear even before their first commit.
        foreach ($this->samsa()->subscriptions()->all() as $group => $topics) {
            $groups[$group] = array_values(array_unique([...($groups[$group] ?? []), ...array_keys($topics)]));
        }

        ksort($groups);
        $rows = [];

        foreach ($groups as $group => $topics) {
            if ($this->option('group') && $this->option('group') !== $group) {
                continue;
            }

            foreach ($topics as $topic) {
                if (($this->option('topic') && $this->option('topic') !== $topic) || $client->storage()->log()->topic($topic) === null) {
                    continue;
                }

                foreach ($client->stats()->partitions($group, $topic) as $row) {
                    if ($this->option('partition') !== null && (int) $this->option('partition') !== $row['partition']) {
                        continue;
                    }

                    $rows[] = ['group' => $group, 'topic' => $topic, ...$row];
                }
            }
        }

        if ($this->option('json')) {
            $this->line((string) json_encode(array_map(fn (array $row) => [
                ...$row,
                'locked_until' => $row['locked_until']?->toIso8601String(),
            ], $rows), JSON_PRETTY_PRINT));

            return self::SUCCESS;
        }

        if ($rows === []) {
            $this->components->info('No consumer groups yet.');

            return self::SUCCESS;
        }

        $this->table(
            ['Group', 'Topic', 'Part', 'Latest', 'Committed', 'Lag', 'Owner', 'Lease until'],
            array_map(fn (array $row) => [
                $row['group'],
                $row['topic'],
                $row['partition'],
                $row['latest'] ?? '-',
                $row['committed'] ?? '-',
                $row['lag'] > 0 && $row['owner'] === null ? "<fg=yellow>{$row['lag']} (unowned)</>" : $row['lag'],
                $row['owner'] ?? '-',
                $row['locked_until']?->format('H:i:s') ?? '-',
            ], $rows),
        );

        return self::SUCCESS;
    }
}
