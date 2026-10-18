<?php

namespace Siberfx\Samsa\Tests\Feature;

use Illuminate\Support\Facades\DB;
use PHPUnit\Framework\AssertionFailedError;
use RuntimeException;
use Siberfx\Samsa\Data\OutgoingRecord;
use Siberfx\Samsa\Data\Record;
use Siberfx\Samsa\Facades\Samsa;
use Siberfx\Samsa\SamsaManager;
use Siberfx\Samsa\Tests\TestCase;

final class FakeTest extends TestCase
{
    public function test_it_records_instead_of_writing(): void
    {
        $fake = Samsa::fake();

        Samsa::produce('orders', ['id' => 1], key: 42, eventType: 'order.placed');
        Samsa::topic('orders')->key('43')->produce(['id' => 2]);
        Samsa::produceBatch('payments', [new OutgoingRecord('p', ['amount' => 5])]);

        Samsa::assertProduced('orders', fn (OutgoingRecord $r) => $r->payload['id'] === 1 && $r->eventType === 'order.placed');
        Samsa::assertProducedTimes('orders', 2);
        Samsa::assertProducedCount('payments', 1);
        Samsa::assertProducedWithKey('orders', 42);
        Samsa::assertNotProduced('refunds');
        Samsa::assertNotProduced('orders', fn (OutgoingRecord $r) => $r->payload['id'] === 3);

        $this->assertSame([1, 2], $fake->produced('orders')->map(fn (OutgoingRecord $r) => $r->payload['id'])->all());
        $this->assertNull($this->app->make(SamsaManager::class)->storage()->log()->topic('orders'), 'nothing was written');
    }

    public function test_failed_assertions_fail(): void
    {
        Samsa::fake();

        $this->expectException(AssertionFailedError::class);

        Samsa::assertProduced('orders');
    }

    public function test_assert_nothing_produced(): void
    {
        Samsa::fake();
        Samsa::assertNothingProduced();

        Samsa::produce('orders', ['id' => 1]);

        $this->expectException(AssertionFailedError::class);
        Samsa::assertNothingProduced();
    }

    public function test_only_selected_topics_can_be_faked(): void
    {
        Samsa::createTopic('payments', 1);
        Samsa::fake(['orders']);

        Samsa::produce('orders', ['id' => 1]);
        Samsa::produce('payments', ['id' => 2]);

        Samsa::assertProducedCount('orders', 1);
        Samsa::assertNotProduced('payments');
        $this->assertSame(1, Samsa::stats()->lag('g', 'payments'), 'not faked: written for real');
    }

    public function test_after_commit_is_faked_and_honours_rollbacks(): void
    {
        Samsa::fake();

        try {
            DB::transaction(function () {
                Samsa::afterCommit()->produce('orders', ['id' => 1]);

                throw new RuntimeException('rollback');
            });
        } catch (RuntimeException) {
        }

        DB::transaction(fn () => Samsa::afterCommit()->produce('orders', ['id' => 2]));

        Samsa::assertProducedTimes('orders', 1);
        Samsa::assertProduced('orders', fn (OutgoingRecord $r) => $r->payload['id'] === 2);
    }

    public function test_named_drivers_are_faked_too(): void
    {
        Samsa::fake();

        Samsa::driver('memory')->produce('orders', ['id' => 1]);

        Samsa::assertProducedCount('orders', 1);
    }

    public function test_consume_until_empty_redeliver_and_assert_dead_lettered(): void
    {
        Samsa::createTopic('orders', 2);
        $seen = [];

        Samsa::subscribe('billing', 'orders', function (Record $record) use (&$seen) {
            if ($record->payload['bad'] ?? false) {
                throw new RuntimeException('bad record');
            }

            $seen[] = $record->payload['n'];
        });

        Samsa::produce('orders', ['n' => 1], key: 'a');
        Samsa::produce('orders', ['n' => 2, 'bad' => true], key: 'a');

        $this->assertSame(2, Samsa::consumeUntilEmpty('billing', 'orders'));
        Samsa::assertDeadLettered('billing', 'orders', fn ($failed) => str_contains($failed->exception, 'bad record'));

        Samsa::redeliver('billing', 'orders');
        Samsa::consumeUntilEmpty('billing', ['orders']);

        $this->assertSame([1, 1], $seen);
    }
}
