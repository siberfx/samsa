<?php

namespace Siberfx\Samsa\Tests\Integration;

use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;
use Siberfx\Samsa\Drivers\Nats\NatsDriver;
use Siberfx\Samsa\Facades\Samsa;
use Siberfx\Samsa\Tests\TestCase;
use Symfony\Component\Process\PhpExecutableFinder;
use Symfony\Component\Process\Process;

/**
 * Real multi-process tests. They need a database server shared by processes:
 *
 *   SAMSA_TEST_DB_CONNECTION=mysql DB_DATABASE=samsa_test DB_USERNAME=root vendor/bin/phpunit --testsuite Integration
 */
final class ConcurrencyTest extends TestCase
{
    private const PARTITIONS = 4;

    protected function setUp(): void
    {
        parent::setUp();

        if (! in_array(getenv('SAMSA_TEST_DB_CONNECTION'), ['mysql', 'mariadb', 'pgsql'], true)) {
            $this->markTestSkipped('Set SAMSA_TEST_DB_CONNECTION=mysql|mariadb|pgsql to run the multi-process tests.');
        }

        if ($this->onNats()) {
            if (! getenv('NATS_HOST')) {
                $this->markTestSkipped('SAMSA_TEST_DRIVER=nats needs NATS_HOST.');
            }

            $this->natsPrefix = 'samsarace'.bin2hex(random_bytes(3));
            config([
                'samsa.default' => 'nats',
                'samsa.drivers.nats' => ['host' => getenv('NATS_HOST'), 'prefix' => $this->natsPrefix, 'storage' => 'memory', 'timeout' => 5.0],
            ]);
        }

        Samsa::createTopic('race', self::PARTITIONS);

        if ($this->onNats()) {
            $this->assertInstanceOf(NatsDriver::class, Samsa::storage(), 'the parent must use NATS too');
        }

        Schema::dropIfExists('samsa_it_consumed');
        Schema::create('samsa_it_consumed', function (Blueprint $t) {
            $t->id();
            $t->string('member');
            $t->unsignedSmallInteger('partition');
            $t->unsignedBigInteger('offset');
            $t->string('writer');
            $t->unsignedInteger('seq');
        });
    }

    private ?string $natsPrefix = null;

    private function onNats(): bool
    {
        return getenv('SAMSA_TEST_DRIVER') === 'nats';
    }

    protected function tearDown(): void
    {
        if ($this->natsPrefix !== null) {
            $storage = Samsa::storage();

            if ($storage instanceof NatsDriver) {
                $storage->destroy();
            }
        }

        Schema::dropIfExists('samsa_it_consumed');

        parent::tearDown();
    }

    public function test_concurrent_producers_never_leave_gaps_or_reorder_keys(): void
    {
        $writers = 8;
        $perWriter = 300;

        $processes = array_map(fn (int $w) => $this->child('produce', "w{$w}", $perWriter), range(1, $writers));
        $this->waitFor($processes);

        $total = 0;
        $perWriterCount = [];

        for ($p = 0; $p < self::PARTITIONS; $p++) {
            $records = Samsa::storage()->log()->read('race', $p, null, 100_000);
            $total += count($records);

            foreach ($records as $record) {
                $perWriterCount[$record->payload['writer']] = ($perWriterCount[$record->payload['writer']] ?? 0) + 1;
            }

            $offsets = array_map(fn ($r) => (int) $r->offset, $records);

            if ($this->onNats()) {
                // JetStream sequences are stream-global: increasing per partition, not contiguous.
                $sorted = array_values(array_unique($offsets));
                sort($sorted);
                $this->assertSame($sorted, $offsets, "partition {$p}: offsets must strictly increase");
            } else {
                $this->assertSame(range(0, count($records) - 1), $offsets, "partition {$p}: offsets must be contiguous from 0, with no gaps or duplicates");
            }

            $lastSeq = [];

            foreach ($records as $record) {
                $key = $record->key;
                $this->assertGreaterThan($lastSeq[$key] ?? -1, $record->payload['seq'], "key {$key} was reordered");
                $lastSeq[$key] = $record->payload['seq'];
            }
        }

        ksort($perWriterCount);
        $this->assertSame($writers * $perWriter, $total, 'every record was written exactly once: '.json_encode([
            'per_writer' => $perWriterCount,
            'outputs' => array_map(fn (Process $p) => $p->getExitCode().' '.$p->getOutput().$p->getErrorOutput(), $processes),
        ], JSON_PRETTY_PRINT));
    }

    public function test_workers_share_partitions_survive_a_kill_and_lose_nothing(): void
    {
        for ($i = 0; $i < 400; $i++) {
            Samsa::produce('race', ['writer' => 'w', 'seq' => $i], key: 'k'.($i % 40));
        }

        $a = $this->child('consume', 'race-group', 30);
        usleep(1_500_000);
        $b = $this->child('consume', 'race-group', 30);
        $c = $this->child('consume', 'race-group', 30);
        usleep(2_500_000);

        $a->stop(0); // hard kill: no graceful release, its leases must expire
        $this->waitFor([$b, $c]);

        $rows = DB::table('samsa_it_consumed')->get();
        $distinct = $rows->map(fn ($r) => "{$r->partition}:{$r->offset}")->unique();

        $this->assertSame(400, $distinct->count(), 'every record was processed at least once: '.json_encode([
            'status' => Samsa::stats()->partitions('race-group', 'race'),
            'per_member' => $rows->countBy('member'),
            'per_partition' => $rows->countBy('partition'),
            'b' => $b->getOutput().$b->getErrorOutput(),
        ], JSON_PRETTY_PRINT));
        $this->assertSame(0, Samsa::stats()->lag('race-group', 'race'), 'everything was committed in the end');
        $this->assertGreaterThanOrEqual(2, $rows->pluck('member')->unique()->count(), 'the work was shared');

        // Only the killed member's uncommitted in-flight batches may be redelivered.
        $this->assertLessThanOrEqual(self::PARTITIONS * 10, $rows->count() - 400, 'duplicates are bounded by batch size per partition');

        // Within a partition, each member saw records in offset order.
        foreach ($rows->groupBy(fn ($r) => "{$r->member}|{$r->partition}") as $group => $memberRows) {
            $offsets = $memberRows->sortBy('id')->pluck('offset')->map(fn ($o) => (int) $o)->values()->all();
            $sorted = $offsets;
            sort($sorted);
            $this->assertSame($sorted, $offsets, "{$group} processed out of order");
        }
    }

    private function child(string $mode, string $name, int $amount): Process
    {
        $php = (new PhpExecutableFinder)->find(false) ?: 'php';

        $process = new Process(
            [$php, __DIR__.'/bin/child.php', $mode, $name, (string) $amount],
            null,
            [
                'SAMSA_TEST_DB_CONNECTION' => getenv('SAMSA_TEST_DB_CONNECTION'),
                'DB_HOST' => getenv('DB_HOST') ?: '127.0.0.1',
                'DB_PORT' => getenv('DB_PORT') ?: '',
                'DB_DATABASE' => getenv('DB_DATABASE') ?: 'samsa_test',
                'DB_USERNAME' => getenv('DB_USERNAME') ?: 'root',
                'DB_PASSWORD' => getenv('DB_PASSWORD') ?: '',
                'SAMSA_TEST_DRIVER' => getenv('SAMSA_TEST_DRIVER') ?: '',
                'NATS_HOST' => getenv('NATS_HOST') ?: '',
                'SAMSA_NATS_PREFIX' => $this->natsPrefix ?? '',
            ],
            null,
            120,
        );

        $process->start();

        return $process;
    }

    /**
     * @param  list<Process>  $processes
     */
    private function waitFor(array $processes): void
    {
        foreach ($processes as $process) {
            $process->wait();
            $this->assertSame(0, $process->getExitCode(), $process->getErrorOutput().$process->getOutput());
        }
    }
}
