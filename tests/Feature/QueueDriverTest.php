<?php

namespace Siberfx\Samsa\Tests\Feature;

use Carbon\CarbonImmutable;
use Illuminate\Support\Facades\Queue;
use Siberfx\Samsa\Data\OutgoingRecord;
use Siberfx\Samsa\Facades\Samsa;
use Siberfx\Samsa\Queue\SamsaQueue;
use Siberfx\Samsa\Tests\Fixtures\FailingJob;
use Siberfx\Samsa\Tests\Fixtures\Journal;
use Siberfx\Samsa\Tests\Fixtures\RecordingJob;
use Siberfx\Samsa\Tests\Fixtures\ReleasingJob;
use Siberfx\Samsa\Tests\TestCase;

final class QueueDriverTest extends TestCase
{
    protected function defineEnvironment($app): void
    {
        parent::defineEnvironment($app);

        $app['config']->set('queue.default', 'samsa');
        $app['config']->set('queue.failed.driver', 'null');
        $app['config']->set('queue.connections.samsa', [
            'driver' => 'samsa',
            'topic' => 'laravel-jobs',
            'group' => 'laravel-queue',
            'lease_seconds' => 30,
        ]);
    }

    protected function setUp(): void
    {
        parent::setUp();

        Journal::reset();
        Samsa::createTopic('laravel-jobs', 4);
    }

    protected function tearDown(): void
    {
        CarbonImmutable::setTestNow();

        parent::tearDown();
    }

    private function work(array $options = []): void
    {
        $this->artisan('queue:work', ['connection' => 'samsa', '--stop-when-empty' => true, '--sleep' => 0, ...$options])->assertSuccessful();
    }

    public function test_dispatched_jobs_run_on_samsa(): void
    {
        RecordingJob::dispatch(1);
        RecordingJob::dispatch(2);

        $this->assertInstanceOf(SamsaQueue::class, Queue::connection('samsa'));
        $this->assertSame(2, Queue::connection('samsa')->size());

        $this->work();

        $this->assertEqualsCanonicalizing([1, 2], Journal::$jobs);
        $this->assertSame(0, Queue::connection('samsa')->size(), 'handled jobs are committed');
    }

    public function test_jobs_with_the_same_key_run_in_dispatch_order(): void
    {
        foreach (range(1, 20) as $n) {
            RecordingJob::dispatch($n, 'product-7');
        }

        $this->work();

        $this->assertSame(range(1, 20), Journal::$jobs);
    }

    public function test_released_jobs_are_retried_with_an_incremented_attempt(): void
    {
        ReleasingJob::dispatch();

        $this->work();

        $this->assertSame([2], Journal::$jobs, 'the job ran again as attempt 2');
        $this->assertSame(2, Journal::$attempts);
    }

    public function test_released_jobs_with_a_delay_wait_in_the_delayed_store(): void
    {
        CarbonImmutable::setTestNow('2026-10-19 10:00:00');
        ReleasingJob::dispatch(60);

        $this->work();
        $this->assertSame([], Journal::$jobs);
        $this->assertSame(1, Queue::connection('samsa')->delayedSize());

        $this->artisan('samsa:release-delayed')->assertSuccessful();
        $this->assertSame(1, Queue::connection('samsa')->delayedSize(), 'not due yet');

        CarbonImmutable::setTestNow('2026-10-19 10:01:01');
        $this->artisan('samsa:release-delayed')->expectsOutputToContain('1 delayed record(s) released')->assertSuccessful();
        $this->work();

        $this->assertSame([2], Journal::$jobs);
    }

    public function test_delayed_dispatch(): void
    {
        CarbonImmutable::setTestNow('2026-10-19 10:00:00');
        RecordingJob::dispatch(1)->delay(now()->addMinutes(5));

        $this->assertSame(0, Queue::connection('samsa')->size());
        $this->assertSame(1, Queue::connection('samsa')->delayedSize());

        CarbonImmutable::setTestNow('2026-10-19 10:05:00');
        Samsa::releaseDelayed();
        $this->work();

        $this->assertSame([1], Journal::$jobs);
    }

    public function test_failing_jobs_use_tries_then_fail(): void
    {
        FailingJob::dispatch();

        $this->work();

        $this->assertSame(2, Journal::$attempts, '$tries = 2');
        $this->assertSame(['failed: always fails'], Journal::$jobs);
        $this->assertSame(0, Queue::connection('samsa')->size(), 'the failed job was committed past');
    }

    public function test_named_queues_map_to_topics(): void
    {
        Samsa::createTopic('media', 1);

        RecordingJob::dispatch(1)->onQueue('media');

        $this->assertSame(1, Queue::connection('samsa')->size('media'));
        $this->assertSame(0, Queue::connection('samsa')->size());

        $this->work(['--queue' => 'media']);

        $this->assertSame([1], Journal::$jobs);
    }

    public function test_clear_skips_pending_jobs(): void
    {
        RecordingJob::dispatch(1);
        RecordingJob::dispatch(2);

        $this->assertSame(2, Queue::connection('samsa')->clear('default'));
        $this->work();

        $this->assertSame([], Journal::$jobs);
    }

    public function test_oldest_pending_job_time(): void
    {
        CarbonImmutable::setTestNow('2026-10-19 10:00:00');
        RecordingJob::dispatch(1);
        CarbonImmutable::setTestNow('2026-10-19 11:00:00');
        RecordingJob::dispatch(2);

        $this->assertSame(CarbonImmutable::parse('2026-10-19 10:00:00')->getTimestamp(), Queue::connection('samsa')->creationTimeOfOldestPendingJob());
        $this->assertSame(0, Queue::connection('samsa')->reservedSize());
    }

    public function test_the_fake_captures_queued_jobs(): void
    {
        Samsa::fake();

        RecordingJob::dispatch(5, 'k');

        Samsa::assertProduced('laravel-jobs', fn (OutgoingRecord $r) => $r->key === 'k' && $r->eventType === SamsaQueue::EVENT_TYPE);
    }
}
