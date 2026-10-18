<?php

/**
 * Child process for the concurrency tests. Boots a Testbench app against the
 * same database as the parent test (SAMSA_TEST_DB_CONNECTION + DB_* env).
 *
 *   php child.php produce <writer> <count>
 *   php child.php consume <group> <seconds>
 */

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Event;
use Orchestra\Testbench\Foundation\Application;
use Siberfx\Samsa\Consumer\WorkerOptions;
use Siberfx\Samsa\Data\Record;
use Siberfx\Samsa\Facades\Samsa;
use Siberfx\Samsa\SamsaServiceProvider;

require __DIR__.'/../../../vendor/autoload.php';

$app = Application::create(options: ['extra' => ['dont-discover' => ['*']]]);
$app['config']->set('database.default', getenv('SAMSA_TEST_DB_CONNECTION') ?: 'mysql');
$app['config']->set('cache.default', 'array');
$app->register(SamsaServiceProvider::class);

if (getenv('SAMSA_TEST_DRIVER') === 'nats') {
    $app['config']->set('samsa.default', 'nats');
    $app['config']->set('samsa.drivers.nats', [
        'host' => (string) getenv('NATS_HOST'),
        'prefix' => (string) getenv('SAMSA_NATS_PREFIX'),
        'storage' => 'memory',
        'timeout' => 5.0,
    ]);
}

[, $mode, $name, $amount] = $argv;

if ($mode === 'produce') {
    for ($i = 0; $i < (int) $amount; $i++) {
        // 20 keys per writer: per-key order must survive concurrent writers.
        Samsa::produce('race', ['writer' => $name, 'seq' => $i], key: "{$name}-".($i % 20));
    }

    exit(0);
}

if ($mode === 'consume') {
    $worker = Samsa::worker();
    $worker->setOutput(fn (string $level, string $message) => print (date('H:i:s')." [{$level}] {$message}\n"));
    Event::listen('Siberfx\Samsa\Events\*', function (string $event, array $payload) {
        if (! str_contains($event, 'Record')) {
            echo date('H:i:s').' '.class_basename($event).' '.json_encode($payload[0])."\n";
        }
    });

    Samsa::subscribe('race-group', 'race', function (Record $record) use ($worker) {
        DB::table('samsa_it_consumed')->insert([
            'member' => $worker->memberId(),
            'partition' => $record->partition,
            'offset' => (int) $record->offset,
            'writer' => $record->payload['writer'],
            'seq' => $record->payload['seq'],
        ]);
        usleep(20000); // ~8s of work for one worker, so later workers and the kill happen mid-stream
    });

    $worker->run('race-group', ['race'], new WorkerOptions(
        batchSize: 10,
        sleepMs: 50,
        leaseSeconds: 2,
        heartbeatSeconds: 1,
        maxTime: (int) $amount,
    ));

    exit(0);
}

fwrite(STDERR, "Unknown mode {$mode}\n");
exit(1);
