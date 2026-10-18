<?php

namespace Siberfx\Samsa\Tests\Feature;

use Carbon\CarbonImmutable;
use Illuminate\Support\Facades\Facade;
use RuntimeException;
use Siberfx\Samsa\Consumer\Worker;
use Siberfx\Samsa\Consumer\WorkerOptions;
use Siberfx\Samsa\Data\Record;
use Siberfx\Samsa\Data\RetentionPolicy;
use Siberfx\Samsa\Facades\Samsa;
use Siberfx\Samsa\SamsaManager;
use Siberfx\Samsa\Tests\Fixtures\Journal;
use Siberfx\Samsa\Tests\Fixtures\RecordingHandler;
use Siberfx\Samsa\Tests\TestCase;

final class CommandsTest extends TestCase
{
    protected function setUp(): void
    {
        parent::setUp();

        Journal::reset();
    }

    protected function tearDown(): void
    {
        CarbonImmutable::setTestNow();

        parent::tearDown();
    }

    public function test_topic_create_list_alter_and_delete(): void
    {
        $this->artisan('samsa:topic:create', ['name' => 'orders', '--partitions' => 4])
            ->expectsOutputToContain('created')
            ->assertSuccessful();

        $this->assertSame(4, Samsa::storage()->log()->topic('orders')?->partitions);

        Samsa::produce('orders', ['n' => 1], key: 'a');

        $this->artisan('samsa:topic:list')
            ->expectsTable(['Topic', 'Partitions', 'Records', 'Retention'], [['orders', 4, 1, '7d']])
            ->assertSuccessful();

        $this->artisan('samsa:topic:alter', ['name' => 'orders', '--partitions' => 6])->assertFailed();
        $this->artisan('samsa:topic:alter', ['name' => 'orders', '--partitions' => 6, '--force' => true])->assertSuccessful();
        $this->assertSame(6, Samsa::storage()->log()->topic('orders')?->partitions);

        $this->artisan('samsa:topic:delete', ['name' => 'orders'])->assertFailed();
        $this->artisan('samsa:topic:delete', ['name' => 'orders', '--force' => true])->assertSuccessful();
        $this->assertNull(Samsa::storage()->log()->topic('orders'));
    }

    public function test_topics_from_config(): void
    {
        config(['samsa.topics' => [
            'orders' => ['partitions' => 4],
            'views' => ['partitions' => 2, 'retention' => ['max_age_seconds' => 3600]],
        ]]);

        $this->artisan('samsa:topic:create', ['--from-config' => true])->assertSuccessful();

        $this->assertSame(4, Samsa::storage()->log()->topic('orders')?->partitions);
        $this->assertSame(3600, Samsa::storage()->log()->topic('views')?->retention->maxAgeSeconds);

        config(['samsa.topics.orders.partitions' => 8]);
        $this->app->forgetInstance(SamsaManager::class);
        Facade::clearResolvedInstance('samsa');

        $this->artisan('samsa:topic:create', ['--from-config' => true])->expectsOutputToContain('kept 4 partitions')->assertSuccessful();
        $this->assertSame(4, Samsa::storage()->log()->topic('orders')?->partitions);

        $this->artisan('samsa:topic:create', ['--from-config' => true, '--force' => true])->assertSuccessful();
        $this->assertSame(8, Samsa::storage()->log()->topic('orders')?->partitions);
    }

    public function test_produce_command(): void
    {
        Samsa::createTopic('orders', 2);

        $this->artisan('samsa:produce', ['topic' => 'orders', 'payload' => '{"n": 7}', '--key' => 'a', '--header' => ['tenant:acme'], '--type' => 'order.placed'])
            ->expectsOutputToContain('Produced to orders')
            ->assertSuccessful();

        $this->artisan('samsa:produce', ['topic' => 'orders', 'payload' => '{nope'])->assertFailed();

        $records = [];

        foreach ([0, 1] as $p) {
            $records = [...$records, ...Samsa::storage()->log()->read('orders', $p, null, 10)];
        }

        $this->assertCount(1, $records);
        $this->assertSame(['n' => 7], $records[0]->payload);
        $this->assertSame('acme', $records[0]->header('tenant'));
        $this->assertSame('order.placed', $records[0]->eventType);
    }

    public function test_status_and_groups(): void
    {
        Samsa::createTopic('orders', 2);
        Samsa::subscribe('billing', 'orders', RecordingHandler::class);
        Samsa::produce('orders', ['n' => 1], key: 'a');
        Samsa::produce('orders', ['n' => 2], key: 'a');

        $this->artisan('samsa:status', ['--json' => true])
            ->expectsOutputToContain('"group": "billing"')
            ->assertSuccessful();

        $this->artisan('samsa:status')->assertSuccessful();

        $this->artisan('samsa:consume', ['group' => 'billing', 'topic' => ['orders'], '--stop-when-empty' => true])->assertSuccessful();

        $this->assertSame(0, Samsa::stats()->lag('billing', 'orders'));

        $this->artisan('samsa:groups')
            ->expectsTable(['Group', 'Topic', 'Members', 'Lag'], [['billing', 'orders', 0, 0]])
            ->assertSuccessful();
    }

    public function test_seek_command(): void
    {
        Samsa::createTopic('orders', 1);

        foreach (range(0, 4) as $n) {
            Samsa::produce('orders', ['n' => $n]);
        }

        $this->artisan('samsa:seek', ['group' => 'billing', 'topic' => 'orders', '--to' => 'latest'])->assertSuccessful();
        $this->assertSame('4', Samsa::offsets('billing', 'orders')->committed(0));

        $this->artisan('samsa:seek', ['group' => 'billing', 'topic' => 'orders', '--to' => 'offset:2', '--partition' => 0])->assertSuccessful();
        $this->assertSame('1', Samsa::offsets('billing', 'orders')->committed(0), 'offset:2 means record 2 is delivered next');

        $this->artisan('samsa:seek', ['group' => 'billing', 'topic' => 'orders', '--to' => 'earliest', '--dry-run' => true])->assertSuccessful();
        $this->assertSame('1', Samsa::offsets('billing', 'orders')->committed(0));

        $this->artisan('samsa:seek', ['group' => 'emails', 'topic' => 'orders', '--to' => 'group:billing'])->assertSuccessful();
        $this->assertSame('1', Samsa::offsets('emails', 'orders')->committed(0));

        $this->artisan('samsa:seek', ['group' => 'billing', 'topic' => 'orders'])->assertFailed();
    }

    public function test_seek_to_a_time(): void
    {
        Samsa::createTopic('orders', 1);

        CarbonImmutable::setTestNow('2026-03-01 08:00:00');
        Samsa::produce('orders', ['n' => 0]);
        CarbonImmutable::setTestNow('2026-03-02 08:00:00');
        Samsa::produce('orders', ['n' => 1]);

        $this->artisan('samsa:seek', ['group' => 'billing', 'topic' => 'orders', '--to' => 'time:2026-03-02T00:00:00Z'])->assertSuccessful();

        $this->assertSame('0', Samsa::offsets('billing', 'orders')->committed(0));
    }

    public function test_failed_commands(): void
    {
        Samsa::createTopic('orders', 1);
        $attempts = 0;
        Samsa::subscribe('billing', 'orders', function (Record $record) use (&$attempts) {
            $attempts++;

            if ($attempts <= 3) {
                throw new RuntimeException('gateway down');
            }

            Journal::$records[] = $record;
        });

        Samsa::produce('orders', ['n' => 1], key: 'a');
        $this->artisan('samsa:consume', ['group' => 'billing', 'topic' => ['orders'], '--stop-when-empty' => true])->assertSuccessful();

        $failed = Samsa::deadLetters()->all();
        $this->assertCount(1, $failed);
        $id = $failed[0]->id;

        $this->artisan('samsa:failed')->expectsOutputToContain($id)->assertSuccessful();
        $this->artisan('samsa:failed:show', ['id' => $id])->expectsOutputToContain('gateway down')->assertSuccessful();
        $this->artisan('samsa:failed:show', ['id' => 'missing'])->assertFailed();

        $this->artisan('samsa:failed:retry', ['id' => $id, '--in-place' => true])->expectsOutputToContain('handled')->assertSuccessful();
        $this->assertSame([], Samsa::deadLetters()->all());
        $this->assertSame([1], Journal::payloads());
    }

    public function test_failed_retry_re_produces_with_the_same_message_id(): void
    {
        Samsa::createTopic('orders', 1);
        Samsa::subscribe('billing', 'orders', fn () => throw new RuntimeException('no'));
        Samsa::produce('orders', ['n' => 1], key: 'a');
        $this->artisan('samsa:consume', ['group' => 'billing', 'topic' => ['orders'], '--stop-when-empty' => true]);

        $original = Samsa::storage()->log()->read('orders', 0, null, 1)[0];

        $this->artisan('samsa:failed:retry', ['--all' => true])->expectsOutputToContain('re-produced')->assertSuccessful();

        $records = Samsa::storage()->log()->read('orders', 0, null, 10);
        $this->assertCount(2, $records);
        $this->assertSame($original->messageId(), $records[1]->messageId());
        $this->assertSame('orders/0/0', $records[1]->header('samsa-retry-of'));
        $this->assertSame([], Samsa::deadLetters()->all());
    }

    public function test_failed_forget_and_flush(): void
    {
        Samsa::createTopic('orders', 1);
        Samsa::produce('orders', ['n' => 1]);
        $record = Samsa::storage()->log()->read('orders', 0, null, 1)[0];

        CarbonImmutable::setTestNow('2026-01-01');
        $old = Samsa::deadLetters()->store('billing', $record, new RuntimeException('x'), 1);
        CarbonImmutable::setTestNow('2026-02-01');
        $new = Samsa::deadLetters()->store('billing', $record, new RuntimeException('y'), 1);

        $this->artisan('samsa:failed:flush', ['--older-than' => '7d'])->expectsOutputToContain('1 dead letter')->assertSuccessful();
        $this->assertNull(Samsa::deadLetters()->find($old->id));

        $this->artisan('samsa:failed:forget', ['id' => $new->id])->assertSuccessful();
        $this->artisan('samsa:failed:forget', ['id' => $new->id])->assertFailed();
    }

    public function test_prune_command_respects_the_slowest_consumer(): void
    {
        Samsa::createTopic('orders', 1, new RetentionPolicy(maxRecordsPerPartition: 2, respectSlowestConsumer: true));

        foreach (range(0, 5) as $n) {
            Samsa::produce('orders', ['n' => $n]);
        }

        Samsa::offsets('billing', 'orders')->seekToOffset(0, '2'); // committed = 1

        $this->artisan('samsa:prune')->expectsOutputToContain('2 record(s) deleted')->assertSuccessful();

        Samsa::offsets('billing', 'orders')->seekToLatest();
        $this->artisan('samsa:prune')->expectsOutputToContain('2 record(s) deleted')->assertSuccessful();

        $this->assertSame([4, 5], array_map(fn (Record $r) => $r->payload['n'], Samsa::storage()->log()->read('orders', 0, null, 10)));

        $this->artisan('samsa:prune', ['--dry-run' => true])->expectsOutputToContain('max per partition: 2')->assertSuccessful();
    }

    public function test_restart_command_stops_running_workers(): void
    {
        Samsa::createTopic('orders', 1);
        Samsa::subscribe('billing', 'orders', function () {
            $this->artisan('samsa:restart')->assertSuccessful();
        });
        Samsa::produce('orders', ['n' => 1]);
        Samsa::produce('orders', ['n' => 2]);

        $worker = Samsa::worker();
        $worker->run('billing', ['orders'], new WorkerOptions(batchSize: 1));

        $this->assertNotNull(cache()->get(Worker::RESTART_CACHE_KEY));
        $this->assertSame(1, $worker->processed(), 'the worker exits after the batch in which the restart was requested');
    }

    public function test_tail_command(): void
    {
        Samsa::createTopic('orders', 1);
        Samsa::produce('orders', ['n' => 1], key: 'a', eventType: 'x.y');

        $this->artisan('samsa:tail', ['topic' => 'orders', '--from' => 'earliest', '--limit' => 1])
            ->expectsOutputToContain('type=x.y')
            ->assertSuccessful();
    }
}
