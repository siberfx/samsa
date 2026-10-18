<?php

namespace Siberfx\Samsa\Tests\Feature;

use Carbon\CarbonImmutable;
use Illuminate\Support\Facades\DB;
use RuntimeException;
use Siberfx\Samsa\Idempotency\OnlyOnce;
use Siberfx\Samsa\Testing\RecordFactory;
use Siberfx\Samsa\Tests\TestCase;

final class OnlyOnceTest extends TestCase
{
    public function test_the_callback_runs_once_per_scope_and_message(): void
    {
        $onlyOnce = $this->app->make(OnlyOnce::class);
        $record = RecordFactory::make('orders', ['points' => 50]);
        $runs = 0;

        $this->assertTrue($onlyOnce->run('loyalty', $record, function () use (&$runs) {
            $runs++;
        }));
        $this->assertFalse($onlyOnce->run('loyalty', $record->withDeliveryAttempt(2), function () use (&$runs) {
            $runs++;
        }));
        $this->assertTrue($onlyOnce->run('analytics', $record, function () use (&$runs) {
            $runs++;
        }), 'scopes are independent');

        $this->assertSame(2, $runs);
    }

    public function test_a_failing_callback_does_not_mark_the_message_processed(): void
    {
        $onlyOnce = $this->app->make(OnlyOnce::class);
        $record = RecordFactory::make('orders');

        try {
            $onlyOnce->run('loyalty', $record, fn () => throw new RuntimeException('boom'));
        } catch (RuntimeException) {
        }

        $this->assertTrue($onlyOnce->run('loyalty', $record, fn () => null), 'the marker rolled back with the failure');
    }

    public function test_prune_removes_old_markers(): void
    {
        $onlyOnce = $this->app->make(OnlyOnce::class);

        CarbonImmutable::setTestNow('2026-01-01');
        $onlyOnce->run('s', RecordFactory::make('orders'), fn () => null);
        CarbonImmutable::setTestNow('2026-02-01');
        $onlyOnce->run('s', RecordFactory::make('orders'), fn () => null);

        $this->assertSame(1, $onlyOnce->prune(CarbonImmutable::parse('2026-01-15')));
        $this->assertSame(1, DB::table('samsa_processed')->count());

        CarbonImmutable::setTestNow();
    }
}
