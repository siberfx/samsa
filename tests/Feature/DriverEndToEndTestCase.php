<?php

namespace Siberfx\Samsa\Tests\Feature;

use Carbon\CarbonImmutable;
use Illuminate\Support\Facades\Queue;
use RuntimeException;
use Siberfx\Samsa\Contracts\StorageDriver;
use Siberfx\Samsa\Data\Record;
use Siberfx\Samsa\Facades\Samsa;
use Siberfx\Samsa\SamsaManager;
use Siberfx\Samsa\Tests\Fixtures\Journal;
use Siberfx\Samsa\Tests\Fixtures\RecordingJob;
use Siberfx\Samsa\Tests\Fixtures\ReleasingJob;
use Siberfx\Samsa\Tests\TestCase;

/**
 * The worker, queue driver and delayed records against a network driver.
 */
abstract class DriverEndToEndTestCase extends TestCase
{
    abstract protected function driverName(): string;

    abstract protected function available(): bool;

    /**
     * @return array<string, mixed>
     */
    abstract protected function driverConfig(): array;

    abstract protected function cleanUp(StorageDriver $storage): void;

    protected function setUp(): void
    {
        if (! $this->available()) {
            $this->markTestSkipped("The {$this->driverName()} server isn't configured for the end-to-end tests.");
        }

        parent::setUp();

        Journal::reset();
        Samsa::createTopic('orders', 4);
    }

    protected function tearDown(): void
    {
        CarbonImmutable::setTestNow();

        if ($this->app === null) {
            return; // skipped before the application booted
        }

        $this->cleanUp($this->app->make(SamsaManager::class)->driver($this->driverName())->storage());

        parent::tearDown();
    }

    protected function defineEnvironment($app): void
    {
        parent::defineEnvironment($app);

        $app['config']->set('samsa.default', $this->driverName());
        $app['config']->set("samsa.drivers.{$this->driverName()}", $this->driverConfig());
        $app['config']->set('queue.default', 'samsa');
        $app['config']->set('queue.failed.driver', 'null');
        $app['config']->set('queue.connections.samsa', ['driver' => 'samsa', 'topic' => 'jobs', 'lease_seconds' => 30]);
    }

    public function test_groups_consume_in_order_and_commit(): void
    {
        foreach (range(0, 29) as $n) {
            Samsa::produce('orders', ['n' => $n], key: 'user-'.($n % 3));
        }

        $seen = [];
        Samsa::subscribe('billing', 'orders', function (Record $record) use (&$seen) {
            $seen[$record->key][] = $record->payload['n'];
        });

        $this->assertSame(30, Samsa::consumeUntilEmpty('billing', 'orders'));
        $this->assertSame(0, Samsa::stats()->lag('billing', 'orders'));
        $this->assertSame(range(0, 29, 3), $seen['user-0'], 'per-key order');

        Samsa::produce('orders', ['n' => 99], key: 'user-0');
        $this->assertSame(1, Samsa::consumeUntilEmpty('billing', 'orders'), 'only new records after a restart');
    }

    public function test_retries_and_dead_letters(): void
    {
        $attempts = 0;
        Samsa::subscribe('billing', 'orders', function (Record $record) use (&$attempts) {
            $attempts++;

            if ($record->payload['bad']) {
                throw new RuntimeException('nope');
            }
        });

        Samsa::produce('orders', ['bad' => true], key: 'k');
        Samsa::produce('orders', ['bad' => false], key: 'k');

        Samsa::consumeUntilEmpty('billing', 'orders');

        $this->assertSame(4, $attempts, '3 attempts for the bad record, 1 for the good one');
        Samsa::assertDeadLettered('billing', 'orders', fn ($f) => $f->attempts === 3);
        $this->assertSame(0, Samsa::stats()->lag('billing', 'orders'));

        $failed = Samsa::deadLetters()->all()[0];
        $this->artisan('samsa:failed:retry', ['id' => $failed->id])->assertSuccessful();
        $this->assertSame(1, Samsa::stats()->lag('billing', 'orders'), 're-produced to the topic');
    }

    public function test_seek_replays_from_a_time(): void
    {
        CarbonImmutable::setTestNow('2026-10-01 10:00:00');
        Samsa::produce('orders', ['n' => 'old'], key: 'k');
        CarbonImmutable::setTestNow('2026-10-02 10:00:00');
        Samsa::produce('orders', ['n' => 'new'], key: 'k');

        $seen = [];
        Samsa::subscribe('replay', 'orders', function (Record $record) use (&$seen) {
            $seen[] = $record->payload['n'];
        });

        Samsa::offsets('replay', 'orders')->seekToTimestamp(CarbonImmutable::parse('2026-10-02 00:00:00'));
        Samsa::consumeUntilEmpty('replay', 'orders');

        $this->assertSame(['new'], $seen);
    }

    public function test_queue_driver_and_delayed_jobs(): void
    {
        Samsa::createTopic('jobs', 2);

        RecordingJob::dispatch(1, 'a');
        ReleasingJob::dispatch(30);

        $this->artisan('queue:work', ['connection' => 'samsa', '--stop-when-empty' => true, '--sleep' => 0])->assertSuccessful();
        $this->assertSame([1], Journal::$jobs);
        $this->assertSame(1, Queue::connection('samsa')->delayedSize());

        CarbonImmutable::setTestNow(CarbonImmutable::now()->addMinute());
        $this->artisan('samsa:release-delayed')->assertSuccessful();
        $this->artisan('queue:work', ['connection' => 'samsa', '--stop-when-empty' => true, '--sleep' => 0])->assertSuccessful();

        $this->assertSame([1, 2], Journal::$jobs);
    }

    public function test_status_and_topic_commands(): void
    {
        Samsa::produce('orders', ['n' => 1], key: 'a');

        $this->artisan('samsa:topic:list')->expectsOutputToContain('orders')->assertSuccessful();
        $this->artisan('samsa:seek', ['group' => 'g', 'topic' => 'orders', '--to' => 'latest'])->assertSuccessful();
        $this->artisan('samsa:status', ['--json' => true])->expectsOutputToContain('"group": "g"')->assertSuccessful();
    }
}
