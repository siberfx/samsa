<?php

namespace Siberfx\Samsa\Tests\Feature;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Event;
use Illuminate\Support\Facades\Facade;
use RuntimeException;
use Siberfx\Samsa\Data\OutgoingRecord;
use Siberfx\Samsa\Data\Record;
use Siberfx\Samsa\Events\RecordProduced;
use Siberfx\Samsa\Exceptions\CrossConnectionTransactionException;
use Siberfx\Samsa\Exceptions\SerializationException;
use Siberfx\Samsa\Exceptions\TopicNotFoundException;
use Siberfx\Samsa\Facades\Samsa;
use Siberfx\Samsa\Producer\DefaultPartitioner;
use Siberfx\Samsa\Tests\TestCase;

final class ProducerTest extends TestCase
{
    protected function setUp(): void
    {
        parent::setUp();

        Samsa::createTopic('orders', 8);
    }

    /**
     * @return list<Record>
     */
    private function partitionRecords(string $topic, int $partition): array
    {
        return Samsa::storage()->log()->read($topic, $partition, null, 1000);
    }

    public function test_records_with_the_same_key_are_read_back_in_order(): void
    {
        for ($i = 0; $i < 100; $i++) {
            Samsa::produce('orders', ['n' => $i], key: 'user-42');
        }

        $partition = (new DefaultPartitioner)->partition('user-42', 8);
        $records = $this->partitionRecords('orders', $partition);

        $this->assertCount(100, $records);
        $this->assertSame(range(0, 99), array_map(fn (Record $r) => $r->payload['n'], $records));
    }

    public function test_different_keys_spread_over_partitions_and_each_partition_is_ordered(): void
    {
        for ($i = 0; $i < 200; $i++) {
            Samsa::produce('orders', ['key' => 'k'.($i % 20), 'seq' => $i], key: 'k'.($i % 20));
        }

        $used = 0;

        for ($p = 0; $p < 8; $p++) {
            $records = $this->partitionRecords('orders', $p);
            $used += $records === [] ? 0 : 1;

            $lastSeqPerKey = [];

            foreach ($records as $record) {
                $key = $record->payload['key'];
                $this->assertGreaterThan($lastSeqPerKey[$key] ?? -1, $record->payload['seq']);
                $lastSeqPerKey[$key] = $record->payload['seq'];
            }
        }

        $this->assertGreaterThan(4, $used);
    }

    public function test_standard_headers_are_added_and_can_be_overridden(): void
    {
        config(['app.name' => 'shop']);

        $meta = Samsa::produce('orders', ['n' => 1], key: 'a', headers: ['tenant' => 'acme']);
        $record = $this->partitionRecords('orders', $meta->partition)[0];

        $this->assertSame('acme', $record->header('tenant'));
        $this->assertSame(26, strlen((string) $record->messageId()));
        $this->assertNotNull($record->header('produced_at'));

        $relayed = Samsa::produce('orders', ['n' => 2], key: 'a', headers: ['message_id' => 'kept']);
        $records = $this->partitionRecords('orders', $relayed->partition);
        $this->assertSame('kept', end($records)->messageId());
    }

    public function test_the_fluent_builder(): void
    {
        $meta = Samsa::topic('orders')
            ->key(42)
            ->withHeaders(['a' => 1])
            ->withHeaders(['b' => 2])
            ->eventType('order.placed')
            ->dedupKey('placed-42')
            ->produce(['id' => 42]);

        $again = Samsa::topic('orders')->key(42)->dedupKey('placed-42')->produce(['id' => 42]);

        $this->assertEquals($meta, $again);

        $record = $this->partitionRecords('orders', $meta->partition)[0];
        $this->assertSame('42', $record->key);
        $this->assertSame('order.placed', $record->eventType);
        $this->assertSame(1, $record->header('a'));
        $this->assertSame(2, $record->header('b'));
        $this->assertCount(1, $this->partitionRecords('orders', $meta->partition));
    }

    public function test_batches_keep_input_order_in_the_returned_metadata(): void
    {
        $records = array_map(fn (int $i) => new OutgoingRecord('k'.($i % 3), ['n' => $i]), range(0, 29));

        $metas = Samsa::produceBatch('orders', $records);

        $this->assertCount(30, $metas);

        foreach ($metas as $i => $meta) {
            $stored = array_values(array_filter(
                $this->partitionRecords('orders', $meta->partition),
                fn (Record $r) => $r->offset === $meta->offset,
            ));
            $this->assertSame($i, $stored[0]->payload['n']);
        }
    }

    public function test_unknown_topics_throw_unless_auto_create_is_on(): void
    {
        try {
            Samsa::produce('payments', ['n' => 1]);
            $this->fail('Expected TopicNotFoundException');
        } catch (TopicNotFoundException) {
        }

        config(['samsa.auto_create_topics' => true, 'samsa.topics.payments' => ['partitions' => 3]]);
        $this->app->forgetInstance('samsa');
        Facade::clearResolvedInstance('samsa');

        $meta = Samsa::produce('payments', ['n' => 1]);

        $this->assertSame(3, Samsa::storage()->log()->topic('payments')?->partitions);
        $this->assertSame('0', $meta->offset);
    }

    public function test_invalid_headers_are_rejected(): void
    {
        $this->expectException(SerializationException::class);

        Samsa::produce('orders', ['n' => 1], headers: ['nested' => ['not' => 'scalar']]);
    }

    public function test_producing_inside_a_rolled_back_transaction_leaves_nothing(): void
    {
        try {
            DB::transaction(function () {
                Samsa::produce('orders', ['n' => 1], key: 'a');

                throw new RuntimeException('rollback');
            });
        } catch (RuntimeException) {
        }

        $this->assertSame(0, Samsa::stats()->lag('anyone', 'orders'));

        DB::transaction(fn () => Samsa::produce('orders', ['n' => 2], key: 'a'));
        $this->assertSame(1, Samsa::stats()->lag('anyone', 'orders'));
    }

    public function test_after_commit_produces_only_on_commit(): void
    {
        try {
            DB::transaction(function () {
                Samsa::afterCommit()->produce('orders', ['n' => 1], key: 'a');

                throw new RuntimeException('rollback');
            });
        } catch (RuntimeException) {
        }

        $this->assertSame(0, Samsa::stats()->lag('anyone', 'orders'));

        DB::transaction(function () {
            Samsa::afterCommit()->produce('orders', ['n' => 2], key: 'a');
            $this->assertSame(0, Samsa::stats()->lag('anyone', 'orders'), 'not before the commit');
        });

        $this->assertSame(1, Samsa::stats()->lag('anyone', 'orders'));

        Samsa::afterCommit()->produce('orders', ['n' => 3], key: 'a');
        $this->assertSame(2, Samsa::stats()->lag('anyone', 'orders'), 'immediately without a transaction');
    }

    public function test_producing_to_another_connection_inside_a_transaction_is_refused(): void
    {
        config([
            'database.connections.events' => ['driver' => 'sqlite', 'database' => ':memory:', 'prefix' => ''],
            'samsa.drivers.database.connection' => 'events',
        ]);
        $this->app->forgetInstance('samsa');
        Facade::clearResolvedInstance('samsa');

        $this->expectException(CrossConnectionTransactionException::class);

        DB::transaction(fn () => Samsa::produce('orders', ['n' => 1]));
    }

    public function test_a_record_produced_event_is_dispatched(): void
    {
        Event::fake([RecordProduced::class]);
        $this->app->forgetInstance('samsa');
        Facade::clearResolvedInstance('samsa');

        Samsa::produce('orders', ['n' => 1], key: 'a');

        Event::assertDispatched(RecordProduced::class, fn (RecordProduced $e) => $e->metadata->topic === 'orders' && $e->record->key === 'a');
    }

    public function test_the_memory_driver_works_through_the_same_api(): void
    {
        $memory = Samsa::driver('memory');
        $memory->createTopic('orders', 2);

        $meta = $memory->produce('orders', ['n' => 1], key: 'x');

        $this->assertSame('0', $meta->offset);
        $this->assertSame(0, Samsa::stats()->lag('g', 'orders'), 'the database driver is untouched');
        $this->assertSame(1, $memory->stats()->lag('g', 'orders'));
    }
}
