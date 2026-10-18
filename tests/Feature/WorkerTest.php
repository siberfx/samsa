<?php

namespace Siberfx\Samsa\Tests\Feature;

use Carbon\CarbonImmutable;
use Illuminate\Support\Facades\Event;
use Illuminate\Support\Facades\Facade;
use InvalidArgumentException;
use RuntimeException;
use Siberfx\Samsa\Consumer\PartitionManager;
use Siberfx\Samsa\Consumer\SubscriptionRegistry;
use Siberfx\Samsa\Consumer\WorkerOptions;
use Siberfx\Samsa\Data\Record;
use Siberfx\Samsa\Events\PartitionAssigned;
use Siberfx\Samsa\Events\RecordConsumed;
use Siberfx\Samsa\Events\RecordDeadLettered;
use Siberfx\Samsa\Events\RecordFailed;
use Siberfx\Samsa\Events\WorkerStopping;
use Siberfx\Samsa\Exceptions\LeaseLostException;
use Siberfx\Samsa\Facades\Samsa;
use Siberfx\Samsa\Tests\Fixtures\FiveAttemptsHandler;
use Siberfx\Samsa\Tests\Fixtures\Journal;
use Siberfx\Samsa\Tests\Fixtures\RecordingBatchHandler;
use Siberfx\Samsa\Tests\Fixtures\RecordingHandler;
use Siberfx\Samsa\Tests\Fixtures\SometimesFailingHandler;
use Siberfx\Samsa\Tests\TestCase;

final class WorkerTest extends TestCase
{
    protected function setUp(): void
    {
        parent::setUp();

        Journal::reset();
        Samsa::createTopic('orders', 4);
    }

    protected function tearDown(): void
    {
        CarbonImmutable::setTestNow();

        parent::tearDown();
    }

    private function consume(string $group = 'billing', array $options = [], array $topics = ['orders']): int
    {
        return Samsa::worker()->run($group, $topics, WorkerOptions::fromConfig((array) config('samsa.consumer'), [
            'stopWhenEmpty' => true,
            ...$options,
        ]));
    }

    private function produce(int $count, string $key = 'k', array $extra = []): void
    {
        for ($i = 0; $i < $count; $i++) {
            Samsa::produce('orders', ['n' => $i, ...$extra], key: $key);
        }
    }

    public function test_it_consumes_everything_in_order_and_commits(): void
    {
        Samsa::subscribe('billing', 'orders', RecordingHandler::class);
        $this->produce(25, 'user-1');

        $this->consume();

        $this->assertSame(range(0, 24), Journal::payloads());
        $this->assertSame(0, Samsa::stats()->lag('billing', 'orders'));

        Journal::reset();
        $this->consume();
        $this->assertSame([], Journal::payloads(), 'nothing is redelivered after a commit');

        Samsa::produce('orders', ['n' => 99], key: 'user-1');
        $this->consume();
        $this->assertSame([99], Journal::payloads(), 'only new records are delivered');
    }

    public function test_groups_are_independent(): void
    {
        Samsa::subscribe('billing', 'orders', RecordingHandler::class);
        Samsa::subscribe('emails', 'orders', RecordingHandler::class);
        $this->produce(5);

        $this->consume('billing');
        $this->assertCount(5, Journal::$records);

        $this->consume('emails');
        $this->assertCount(10, Journal::$records, 'a second group sees every record again');
    }

    public function test_closures_can_be_handlers(): void
    {
        $seen = [];
        Samsa::subscribe('audit', 'orders', function (Record $record) use (&$seen) {
            $seen[] = $record->payload['n'];
        });
        $this->produce(3);

        $this->consume('audit');

        $this->assertSame([0, 1, 2], $seen);
    }

    public function test_failed_attempts_are_retried_in_place(): void
    {
        Samsa::subscribe('billing', 'orders', SometimesFailingHandler::class);
        Samsa::produce('orders', ['n' => 0, 'fail_until_attempt' => 2], key: 'k');
        Samsa::produce('orders', ['n' => 1], key: 'k');

        $this->consume();

        $this->assertSame([0, 1], Journal::payloads(), 'order is kept: record 1 waits for record 0');
        $this->assertSame(3, Journal::$records[0]->deliveryAttempt);
        $this->assertSame(4, Journal::$attempts);
        $this->assertSame([], Samsa::deadLetters()->all());
    }

    public function test_records_that_keep_failing_are_dead_lettered_and_skipped(): void
    {
        Event::fake([RecordDeadLettered::class, RecordFailed::class]);
        $this->rebuildSamsa();

        Samsa::subscribe('billing', 'orders', SometimesFailingHandler::class);
        Samsa::produce('orders', ['n' => 0, 'fail' => true], key: 'k');
        Samsa::produce('orders', ['n' => 1], key: 'k');

        $this->consume();

        $this->assertSame([1], Journal::payloads());
        $this->assertSame(0, Samsa::stats()->lag('billing', 'orders'), 'the dead letter is committed past');

        $failed = Samsa::deadLetters()->all();
        $this->assertCount(1, $failed);
        $this->assertSame(3, $failed[0]->attempts);
        $this->assertSame(0, $failed[0]->record->payload['n']);
        $this->assertStringContainsString('record 0 always fails', $failed[0]->exception);

        Event::assertDispatchedTimes(RecordFailed::class, 3);
        Event::assertDispatched(RecordDeadLettered::class);
    }

    public function test_unrecoverable_records_skip_retries(): void
    {
        Samsa::subscribe('billing', 'orders', SometimesFailingHandler::class);
        Samsa::produce('orders', ['n' => 0, 'unrecoverable' => true], key: 'k');

        $this->consume();

        $this->assertSame(1, Journal::$attempts);
        $this->assertSame(1, Samsa::deadLetters()->all()[0]->attempts);
    }

    public function test_the_retry_attribute_sets_the_attempts(): void
    {
        Samsa::subscribe('billing', 'orders', FiveAttemptsHandler::class);
        Samsa::produce('orders', ['n' => 0], key: 'k');

        $this->consume();

        $this->assertSame(5, Journal::$attempts);
    }

    public function test_without_dead_letters_the_worker_stops_and_does_not_commit(): void
    {
        config(['samsa.dead_letter.enabled' => false]);
        Samsa::subscribe('billing', 'orders', SometimesFailingHandler::class);
        Samsa::produce('orders', ['n' => 0, 'fail' => true], key: 'k');

        try {
            $this->consume();
            $this->fail('The worker should have thrown.');
        } catch (RuntimeException $e) {
            $this->assertSame('record 0 always fails', $e->getMessage());
        }

        $this->assertSame(1, Samsa::stats()->lag('billing', 'orders'));

        $owners = array_filter(array_column(Samsa::stats()->partitions('billing', 'orders'), 'owner'));
        $this->assertSame([], $owners, 'leases are released when the worker dies');
    }

    public function test_batch_handlers_receive_ordered_batches_per_partition(): void
    {
        Samsa::subscribe('index', 'orders', RecordingBatchHandler::class);
        $this->produce(7, 'same-key');

        $this->consume('index', ['batchSize' => 3]);

        $this->assertSame([3, 3, 1], array_map('count', Journal::$batches));
        $this->assertSame(range(0, 6), Journal::payloads());
    }

    public function test_a_failing_batch_falls_back_to_single_records(): void
    {
        Samsa::subscribe('index', 'orders', RecordingBatchHandler::class);
        Samsa::produce('orders', ['n' => 0], key: 'k');
        Samsa::produce('orders', ['n' => 1, 'fail' => true], key: 'k');
        Samsa::produce('orders', ['n' => 2], key: 'k');

        $this->consume('index', ['batchSize' => 10]);

        $this->assertSame([0, 2], Journal::payloads());
        $this->assertCount(1, Samsa::deadLetters()->all());
        $this->assertSame(1, Samsa::deadLetters()->all()[0]->record->payload['n']);
        $this->assertSame(0, Samsa::stats()->lag('index', 'orders'));
    }

    public function test_a_new_group_can_start_from_the_latest_offset(): void
    {
        Samsa::subscribe('fresh', 'orders', RecordingHandler::class);
        $this->produce(5);

        $this->consume('fresh', ['startFrom' => 'latest']);
        $this->assertSame([], Journal::payloads());

        Samsa::produce('orders', ['n' => 'new'], key: 'k');
        $this->consume('fresh', ['startFrom' => 'latest']);
        $this->assertSame(['new'], Journal::payloads());
    }

    public function test_once_processes_one_batch_per_partition(): void
    {
        Samsa::subscribe('billing', 'orders', RecordingHandler::class);
        $this->produce(5, 'k');

        $this->consume('billing', ['once' => true, 'batchSize' => 2, 'stopWhenEmpty' => false]);

        $this->assertSame([0, 1], Journal::payloads());
        $this->assertSame(3, Samsa::stats()->lag('billing', 'orders'));
    }

    public function test_max_records_stops_the_worker_and_commits_what_was_done(): void
    {
        Samsa::subscribe('billing', 'orders', RecordingHandler::class);
        $this->produce(10, 'k');

        $this->consume('billing', ['maxRecords' => 4]);

        $this->assertSame([0, 1, 2, 3], Journal::payloads());
        $this->assertSame(6, Samsa::stats()->lag('billing', 'orders'));
    }

    public function test_record_commit_mode_commits_each_record(): void
    {
        $worker = Samsa::worker();

        Samsa::subscribe('billing', 'orders', function (Record $record) use ($worker) {
            Journal::$records[] = $record;

            if ($record->payload['n'] === 2) {
                $worker->stop('test');
            }
        });

        $this->produce(6, 'k');

        $worker->run('billing', ['orders'], new WorkerOptions(commit: 'record', batchSize: 100));

        $this->assertSame([0, 1, 2], Journal::payloads());
        $this->assertSame(3, Samsa::stats()->lag('billing', 'orders'));
    }

    public function test_worker_events(): void
    {
        Event::fake([PartitionAssigned::class, RecordConsumed::class, WorkerStopping::class]);
        $this->rebuildSamsa();

        Samsa::subscribe('billing', 'orders', RecordingHandler::class);
        $this->produce(2);

        $this->consume();

        Event::assertDispatchedTimes(PartitionAssigned::class, 4);
        Event::assertDispatchedTimes(RecordConsumed::class, 2);
        Event::assertDispatched(WorkerStopping::class, fn (WorkerStopping $e) => $e->reason === 'empty');
    }

    public function test_a_missing_subscription_is_reported(): void
    {
        $this->expectException(InvalidArgumentException::class);

        $this->consume('nobody');
    }

    public function test_subscriptions_from_config(): void
    {
        config(['samsa.subscriptions' => ['billing' => ['orders' => RecordingHandler::class]]]);
        $this->app->forgetInstance(SubscriptionRegistry::class);
        $this->produce(1);

        $this->consume();

        $this->assertCount(1, Journal::$records);
    }

    public function test_partitions_are_shared_fairly_and_rebalanced(): void
    {
        Samsa::createTopic('wide', 8);
        $storage = Samsa::storage();
        $options = new WorkerOptions;

        $a = new PartitionManager($storage, 'billing', 'wide', 'member-a', $options);
        $b = new PartitionManager($storage, 'billing', 'wide', 'member-b', $options);

        $a->rebalance();
        $this->assertCount(8, $a->owned(), 'a lone member takes everything');

        $b->rebalance();
        $this->assertSame([], $b->owned(), 'nothing is free yet');

        $changes = $a->rebalance();
        $this->assertCount(4, $changes['revoked'], 'a releases its surplus once b has joined');

        $b->rebalance();
        $this->assertCount(4, $a->owned());
        $this->assertCount(4, $b->owned());
        $this->assertSame([], array_intersect($a->owned(), $b->owned()), 'no partition has two owners');
        $this->assertSame(range(0, 7), $this->sorted([...$a->owned(), ...$b->owned()]));
    }

    public function test_a_crashed_members_partitions_are_taken_over_after_the_lease_expires(): void
    {
        Samsa::createTopic('wide', 4);
        $storage = Samsa::storage();
        $options = new WorkerOptions(leaseSeconds: 30, heartbeatSeconds: 10);

        CarbonImmutable::setTestNow('2026-01-01 00:00:00');
        $a = new PartitionManager($storage, 'billing', 'wide', 'member-a', $options);
        $a->rebalance();
        $storage->offsets()->commit('billing', 'wide', 0, '7', 'member-a');

        // member-a is killed: no release, no more heartbeats.
        $b = new PartitionManager($storage, 'billing', 'wide', 'member-b', $options);
        $b->rebalance();
        $this->assertSame([], $b->owned());

        CarbonImmutable::setTestNow('2026-01-01 00:00:31');
        $b->rebalance();
        $this->assertSame([0, 1, 2, 3], $b->owned());
        $this->assertSame('7', $storage->offsets()->committed('billing', 'wide', 0), 'b resumes from the last commit');

        $this->expectException(LeaseLostException::class);
        $storage->offsets()->commit('billing', 'wide', 0, '9', 'member-a');
    }

    public function test_explicit_partitions_disable_fair_share(): void
    {
        Samsa::createTopic('wide', 8);
        $manager = new PartitionManager(Samsa::storage(), 'billing', 'wide', 'member-a', new WorkerOptions(partitions: [1, 5, 99]));

        $manager->rebalance();

        $this->assertSame([1, 5], $manager->owned());
    }

    public function test_seek_replays_records(): void
    {
        Samsa::subscribe('billing', 'orders', RecordingHandler::class);
        $this->produce(3, 'k');
        $this->consume();

        Journal::reset();
        Samsa::offsets('billing', 'orders')->seekToEarliest();
        $this->consume();

        $this->assertSame([0, 1, 2], Journal::payloads());
    }

    public function test_the_consume_command(): void
    {
        Samsa::subscribe('billing', 'orders', RecordingHandler::class);
        $this->produce(3);

        $this->artisan('samsa:consume', ['group' => 'billing', 'topic' => ['orders'], '--stop-when-empty' => true, '--batch' => 2])
            ->assertSuccessful();

        $this->assertCount(3, Journal::$records);
    }

    /**
     * @param  list<int>  $values
     * @return list<int>
     */
    private function sorted(array $values): array
    {
        sort($values);

        return $values;
    }

    private function rebuildSamsa(): void
    {
        $this->app->forgetInstance('samsa');
        Facade::clearResolvedInstance('samsa');
    }
}
