<?php

namespace Siberfx\Samsa\Testing;

use Carbon\CarbonImmutable;
use PHPUnit\Framework\Attributes\After;
use RuntimeException;
use Siberfx\Samsa\Contracts\StorageDriver;
use Siberfx\Samsa\Data\OutgoingRecord;
use Siberfx\Samsa\Data\Record;
use Siberfx\Samsa\Data\RetentionPolicy;
use Siberfx\Samsa\Data\TopicConfig;
use Siberfx\Samsa\Exceptions\InvalidTopicException;
use Siberfx\Samsa\Exceptions\LeaseLostException;
use Siberfx\Samsa\Exceptions\TopicNotFoundException;

/**
 * The behaviour every StorageDriver must have. Use it in a PHPUnit test case:
 *
 *     final class MyDriverTest extends TestCase
 *     {
 *         use StorageDriverContract;
 *
 *         protected function driver(): StorageDriver { return new MyDriver(...); }
 *     }
 *
 * Tests that need time to pass use Carbon's test clock, so drivers must take
 * "now" from Carbon (or from a clock that honours Carbon::setTestNow()).
 */
trait StorageDriverContract
{
    abstract protected function driver(): StorageDriver;

    private ?StorageDriver $contractDriver = null;

    #[After]
    protected function tearDownStorageDriverContract(): void
    {
        CarbonImmutable::setTestNow();
    }

    private function samsaDriver(): StorageDriver
    {
        return $this->contractDriver ??= $this->driver();
    }

    private function samsaTopic(string $name = 'orders', int $partitions = 4, ?RetentionPolicy $retention = null): TopicConfig
    {
        return $this->samsaDriver()->log()->ensureTopic(new TopicConfig($name, $partitions, $retention));
    }

    /**
     * @param  array<string, mixed>  $payload
     */
    private function samsaRecord(array $payload, ?string $key = 'k', ?string $dedupKey = null): OutgoingRecord
    {
        return new OutgoingRecord($key, $payload, ['message_id' => bin2hex(random_bytes(8))], $dedupKey, 'test.event');
    }

    // --------------------------------------------------------------- topics

    public function test_ensure_topic_creates_and_is_idempotent(): void
    {
        $created = $this->samsaTopic('orders', 4);
        $again = $this->samsaDriver()->log()->ensureTopic(new TopicConfig('orders', 16, new RetentionPolicy(maxAgeSeconds: 60)));

        $this->assertSame(4, $created->partitions);
        $this->assertSame(4, $again->partitions, 'ensureTopic must never change the partition count');
        $this->assertSame(60, $this->samsaDriver()->log()->topic('orders')?->retention->maxAgeSeconds);
        $this->assertNull($this->samsaDriver()->log()->topic('missing'));
        $this->assertSame(['orders'], array_map(fn (TopicConfig $t) => $t->name, $this->samsaDriver()->log()->topics()));
    }

    public function test_partitions_can_grow_but_not_shrink(): void
    {
        $this->samsaTopic('orders', 2);

        $grown = $this->samsaDriver()->log()->growPartitions('orders', 4);
        $this->assertSame(4, $grown->partitions);

        $meta = $this->samsaDriver()->log()->append('orders', 3, $this->samsaRecord(['n' => 1]));
        $this->assertSame(3, $meta->partition);

        $this->expectException(InvalidTopicException::class);
        $this->samsaDriver()->log()->growPartitions('orders', 1);
    }

    public function test_unknown_topics_and_partitions_are_rejected(): void
    {
        $this->samsaTopic('orders', 2);

        try {
            $this->samsaDriver()->log()->append('missing', 0, $this->samsaRecord([]));
            $this->fail('Expected TopicNotFoundException');
        } catch (TopicNotFoundException) {
        }

        $this->expectException(InvalidTopicException::class);
        $this->samsaDriver()->log()->append('orders', 2, $this->samsaRecord([]));
    }

    public function test_delete_topic_removes_records_and_positions(): void
    {
        $this->samsaTopic('orders', 1);
        $this->samsaDriver()->log()->append('orders', 0, $this->samsaRecord(['n' => 1]));
        $this->samsaDriver()->offsets()->commit('billing', 'orders', 0, '0');

        $this->samsaDriver()->log()->deleteTopic('orders');

        $this->assertNull($this->samsaDriver()->log()->topic('orders'));
        $this->assertFalse($this->samsaDriver()->offsets()->hasPosition('billing', 'orders', 0));
    }

    // ------------------------------------------------------------------ log

    public function test_records_in_a_partition_are_read_back_in_order(): void
    {
        $this->samsaTopic('orders', 4);

        for ($i = 0; $i < 100; $i++) {
            $this->samsaDriver()->log()->append('orders', 2, $this->samsaRecord(['n' => $i], 'user-42'));
        }

        $records = $this->samsaDriver()->log()->read('orders', 2, null, 1000);

        $this->assertCount(100, $records);
        $this->assertSame(range(0, 99), array_map(fn (Record $r) => $r->payload['n'], $records));
        $this->assertSame('user-42', $records[0]->key);
        $this->assertSame('test.event', $records[0]->eventType);
        $this->assertSame([], $this->samsaDriver()->log()->read('orders', 1, null, 10));
    }

    public function test_offsets_increase_and_read_is_strictly_after(): void
    {
        $this->samsaTopic('orders', 1);
        $log = $this->samsaDriver()->log();

        $this->assertNull($log->latestOffset('orders', 0));

        $offsets = [];

        for ($i = 0; $i < 5; $i++) {
            $offsets[] = $log->append('orders', 0, $this->samsaRecord(['n' => $i]))->offset;
        }

        $this->assertSame($offsets[4], $log->latestOffset('orders', 0));

        $after = $log->read('orders', 0, $offsets[1], 10);
        $this->assertSame([2, 3, 4], array_map(fn (Record $r) => $r->payload['n'], $after));

        $limited = $log->read('orders', 0, null, 2);
        $this->assertSame([0, 1], array_map(fn (Record $r) => $r->payload['n'], $limited));

        $this->assertSame(3, $log->countAfter('orders', 0, $offsets[1]));
        $this->assertSame(5, $log->countAfter('orders', 0, null));
        $this->assertSame($offsets[2], $log->offsetBefore('orders', 0, $offsets[3]));
        $this->assertNull($log->offsetBefore('orders', 0, $offsets[0]));
    }

    public function test_batch_append_preserves_order_and_payloads(): void
    {
        $this->samsaTopic('orders', 1);

        $metas = $this->samsaDriver()->log()->appendBatch('orders', 0, [
            $this->samsaRecord(['n' => 'a']),
            $this->samsaRecord(['n' => 'b', 'nested' => ['x' => 1.5, 'y' => null]]),
        ]);

        $this->assertCount(2, $metas);
        $records = $this->samsaDriver()->log()->read('orders', 0, null, 10);
        $this->assertSame(['a', 'b'], array_map(fn (Record $r) => $r->payload['n'], $records));
        $this->assertSame(['x' => 1.5, 'y' => null], $records[1]->payload['nested']);
        $this->assertSame($metas[1]->offset, $records[1]->offset);
    }

    public function test_dedup_key_returns_the_existing_record(): void
    {
        $this->samsaTopic('orders', 2);

        $first = $this->samsaDriver()->log()->append('orders', 0, $this->samsaRecord(['n' => 1], dedupKey: 'once'));
        $second = $this->samsaDriver()->log()->append('orders', 1, $this->samsaRecord(['n' => 2], dedupKey: 'once'));
        $batch = $this->samsaDriver()->log()->appendBatch('orders', 0, [
            $this->samsaRecord(['n' => 3], dedupKey: 'twice'),
            $this->samsaRecord(['n' => 4], dedupKey: 'twice'),
        ]);

        $this->assertEquals($first, $second);
        $this->assertEquals($batch[0], $batch[1]);
        $this->assertSame(2, $this->samsaDriver()->log()->countAfter('orders', 0, null));
        $this->assertSame(0, $this->samsaDriver()->log()->countAfter('orders', 1, null));
    }

    public function test_offset_before_time(): void
    {
        $this->samsaTopic('orders', 1);
        $log = $this->samsaDriver()->log();

        CarbonImmutable::setTestNow('2026-01-01 10:00:00');
        $early = $log->append('orders', 0, $this->samsaRecord(['n' => 0]));
        CarbonImmutable::setTestNow('2026-01-01 12:00:00');
        $log->append('orders', 0, $this->samsaRecord(['n' => 1]));

        $this->assertNull($log->offsetBeforeTime('orders', 0, CarbonImmutable::parse('2026-01-01 09:00:00')));
        $this->assertSame($early->offset, $log->offsetBeforeTime('orders', 0, CarbonImmutable::parse('2026-01-01 11:00:00')));
    }

    public function test_prune_by_count_age_and_protection(): void
    {
        $this->samsaTopic('orders', 1);
        $log = $this->samsaDriver()->log();

        CarbonImmutable::setTestNow('2026-01-01 00:00:00');
        $offsets = [];

        for ($i = 0; $i < 10; $i++) {
            $offsets[] = $log->append('orders', 0, $this->samsaRecord(['n' => $i]))->offset;
        }

        $this->assertSame(0, $log->prune('orders', new RetentionPolicy(maxRecordsPerPartition: 4), [0 => null]));
        $this->assertSame(2, $log->prune('orders', new RetentionPolicy(maxRecordsPerPartition: 4), [0 => $offsets[1]]));
        $this->assertSame(4, $log->prune('orders', new RetentionPolicy(maxRecordsPerPartition: 4)));
        $this->assertSame([6, 7, 8, 9], array_map(fn (Record $r) => $r->payload['n'], $log->read('orders', 0, null, 100)));

        CarbonImmutable::setTestNow('2026-01-03 00:00:00');
        $log->append('orders', 0, $this->samsaRecord(['n' => 10]));

        $this->assertSame(4, $log->prune('orders', new RetentionPolicy(maxAgeSeconds: 86400)));
        $this->assertSame([10], array_map(fn (Record $r) => $r->payload['n'], $log->read('orders', 0, null, 100)));
        $this->assertNotNull($log->latestOffset('orders', 0));
    }

    // -------------------------------------------------------------- offsets

    public function test_commits_seeks_and_positions(): void
    {
        $this->samsaTopic('orders', 2);
        $offsets = $this->samsaDriver()->offsets();

        $this->assertNull($offsets->committed('billing', 'orders', 0));
        $this->assertFalse($offsets->hasPosition('billing', 'orders', 0));

        $offsets->commit('billing', 'orders', 0, '5');
        $this->assertSame('5', $offsets->committed('billing', 'orders', 0));
        $this->assertTrue($offsets->hasPosition('billing', 'orders', 0));

        $offsets->seek('billing', 'orders', 0, null);
        $this->assertNull($offsets->committed('billing', 'orders', 0));
        $this->assertTrue($offsets->hasPosition('billing', 'orders', 0), 'a seek to the beginning is still a stored position');

        $offsets->commit('emails', 'orders', 1, '1');
        $this->assertSame(['billing', 'emails'], $offsets->groups('orders'));
        $this->assertSame(['billing' => ['orders'], 'emails' => ['orders']], $offsets->allGroups());
    }

    public function test_fenced_commit_requires_the_lease(): void
    {
        $this->samsaTopic('orders', 1);
        $driver = $this->samsaDriver();

        $this->assertTrue($driver->leases()->acquire('billing', 'orders', 0, 'member-a', 30));
        $driver->offsets()->commit('billing', 'orders', 0, '3', 'member-a');
        $this->assertSame('3', $driver->offsets()->committed('billing', 'orders', 0));

        $this->expectException(LeaseLostException::class);

        try {
            $driver->offsets()->commit('billing', 'orders', 0, '4', 'member-b');
        } finally {
            $this->assertSame('3', $driver->offsets()->committed('billing', 'orders', 0));
        }
    }

    // --------------------------------------------------------------- leases

    public function test_only_one_member_holds_a_lease_until_it_expires(): void
    {
        $this->samsaTopic('orders', 1);
        $leases = $this->samsaDriver()->leases();

        CarbonImmutable::setTestNow('2026-01-01 00:00:00');

        $this->assertTrue($leases->acquire('billing', 'orders', 0, 'a', 30));
        $this->assertFalse($leases->acquire('billing', 'orders', 0, 'b', 30));
        $this->assertTrue($leases->acquire('billing', 'orders', 0, 'a', 30), 'the owner can renew');
        $this->assertTrue($leases->acquire('emails', 'orders', 0, 'b', 30), 'groups are independent');

        CarbonImmutable::setTestNow('2026-01-01 00:00:31');

        $this->assertTrue($leases->acquire('billing', 'orders', 0, 'b', 30), 'an expired lease can be taken over');
        $this->assertSame('b', $leases->leases('billing', 'orders')[0]->memberId);

        CarbonImmutable::setTestNow('2026-01-01 00:00:32');
        $this->expectException(LeaseLostException::class);
        $this->samsaDriver()->offsets()->commit('billing', 'orders', 0, '1', 'a');
    }

    public function test_release_frees_the_partition(): void
    {
        $this->samsaTopic('orders', 1);
        $leases = $this->samsaDriver()->leases();

        $leases->acquire('billing', 'orders', 0, 'a', 30);
        $leases->release('billing', 'orders', 0, 'b');
        $this->assertFalse($leases->acquire('billing', 'orders', 0, 'b', 30), 'only the owner can release');

        $leases->release('billing', 'orders', 0, 'a');
        $this->assertTrue($leases->acquire('billing', 'orders', 0, 'b', 30));
    }

    public function test_members_expire_without_heartbeats(): void
    {
        $this->samsaTopic('orders', 1);
        $leases = $this->samsaDriver()->leases();

        CarbonImmutable::setTestNow('2026-01-01 00:00:00');
        $leases->heartbeat('billing', 'orders', 'b', 30);
        $leases->heartbeat('billing', 'orders', 'a', 10);
        $this->assertSame(['a', 'b'], $leases->members('billing', 'orders'));

        CarbonImmutable::setTestNow('2026-01-01 00:00:20');
        $this->assertSame(['b'], $leases->members('billing', 'orders'));

        $leases->leave('billing', 'orders', 'b');
        $this->assertSame([], $leases->members('billing', 'orders'));
    }

    // --------------------------------------------------------- dead letters

    public function test_dead_letters_can_be_stored_listed_and_removed(): void
    {
        $this->samsaTopic('orders', 1);
        $log = $this->samsaDriver()->log();
        $store = $this->samsaDriver()->deadLetters();

        $log->append('orders', 0, $this->samsaRecord(['n' => 1], 'k-1'));
        $record = $log->read('orders', 0, null, 1)[0];

        CarbonImmutable::setTestNow('2026-01-01 00:00:00');
        $failed = $store->store('billing', $record, new RuntimeException('boom'), 3);
        CarbonImmutable::setTestNow('2026-01-05 00:00:00');
        $store->store('emails', $record, new RuntimeException('bang'), 1);

        $found = $store->find($failed->id);
        $this->assertNotNull($found);
        $this->assertSame('billing', $found->group);
        $this->assertSame(3, $found->attempts);
        $this->assertSame(['n' => 1], $found->record->payload);
        $this->assertSame('k-1', $found->record->key);
        $this->assertStringContainsString('boom', $found->exception);

        $this->assertCount(2, $store->all());
        $this->assertCount(1, $store->all(group: 'emails'));
        $this->assertCount(0, $store->all(topic: 'other'));

        $this->assertSame(1, $store->flush(CarbonImmutable::parse('2026-01-02 00:00:00')));
        $this->assertNull($store->find($failed->id));
        $this->assertCount(1, $store->all());

        $remaining = $store->all()[0];
        $this->assertTrue($store->forget($remaining->id));
        $this->assertFalse($store->forget($remaining->id));
    }
}
