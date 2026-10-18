<?php

namespace Siberfx\Samsa\Tests\Feature;

use Basis\Nats\Client;
use Carbon\CarbonImmutable;
use Illuminate\Support\Facades\Queue;
use RuntimeException;
use Siberfx\Samsa\Data\Record;
use Siberfx\Samsa\Drivers\Nats\NatsDriver;
use Siberfx\Samsa\Facades\Samsa;
use Siberfx\Samsa\SamsaManager;
use Siberfx\Samsa\Tests\Fixtures\Journal;
use Siberfx\Samsa\Tests\Fixtures\RecordingJob;
use Siberfx\Samsa\Tests\Fixtures\ReleasingJob;
use Siberfx\Samsa\Tests\TestCase;

/**
 * The worker, queue driver and delayed records on NATS JetStream.
 * Needs NATS_HOST (e.g. docker run -p 4222:4222 nats:2.10-alpine --jetstream).
 */
final class NatsEndToEndTest extends TestCase
{
    protected function setUp(): void
    {
        if (! getenv('NATS_HOST') || ! class_exists(Client::class)) {
            $this->markTestSkipped('Set NATS_HOST to run the NATS end-to-end tests.');
        }

        parent::setUp();

        Journal::reset();
        Samsa::createTopic('orders', 4);
    }

    protected function tearDown(): void
    {
        CarbonImmutable::setTestNow();

        $storage = $this->app->make(SamsaManager::class)->driver('nats')->storage();

        if ($storage instanceof NatsDriver) {
            $storage->destroy();
            $storage->connection()->disconnect();
        }

        parent::tearDown();
    }

    protected function defineEnvironment($app): void
    {
        parent::defineEnvironment($app);

        $app['config']->set('samsa.default', 'nats');
        $app['config']->set('samsa.drivers.nats', [
            'host' => (string) getenv('NATS_HOST'),
            'port' => (int) (getenv('NATS_PORT') ?: 4222),
            'prefix' => 'samsae2e'.bin2hex(random_bytes(3)),
            'storage' => 'memory',
            'timeout' => 2.0,
        ]);
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

    public function test_queue_driver_and_delayed_jobs_on_nats(): void
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
