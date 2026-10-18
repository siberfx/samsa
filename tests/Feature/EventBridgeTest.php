<?php

namespace Siberfx\Samsa\Tests\Feature;

use DateTimeImmutable;
use Illuminate\Support\Facades\DB;
use InvalidArgumentException;
use RuntimeException;
use Siberfx\Samsa\Bridge\EventPublisher;
use Siberfx\Samsa\Bridge\PayloadNormalizer;
use Siberfx\Samsa\Data\OutgoingRecord;
use Siberfx\Samsa\Exceptions\SerializationException;
use Siberfx\Samsa\Facades\Samsa;
use Siberfx\Samsa\Tests\Fixtures\ChatMessageSent;
use Siberfx\Samsa\Tests\Fixtures\NotPublished;
use Siberfx\Samsa\Tests\Fixtures\OrderPlaced;
use Siberfx\Samsa\Tests\Fixtures\OrderShipped;
use Siberfx\Samsa\Tests\Fixtures\OrderStatus;
use Siberfx\Samsa\Tests\TestCase;

final class EventBridgeTest extends TestCase
{
    public function test_attributed_events_are_produced_when_dispatched(): void
    {
        Samsa::fake();

        OrderPlaced::dispatch('o-1', 99.5, OrderStatus::Paid, new DateTimeImmutable('2026-10-19T10:00:00+00:00'));

        Samsa::assertProduced('orders', function (OutgoingRecord $r) {
            return $r->key === 'o-1'
                && $r->eventType === 'order.placed'
                && $r->payload === ['orderId' => 'o-1', 'total' => 99.5, 'status' => 'paid', 'placedAt' => '2026-10-19T10:00:00.000+00:00'];
        });
        Samsa::assertProduced('analytics', fn (OutgoingRecord $r) => $r->key === null);
    }

    public function test_events_without_the_attribute_are_ignored(): void
    {
        Samsa::fake();

        NotPublished::dispatch();
        event('some.string.event', ['payload']);

        Samsa::assertNothingProduced();
    }

    public function test_custom_payload_headers_and_method_keys(): void
    {
        Samsa::fake();

        OrderShipped::dispatch('o-7');

        Samsa::assertProduced('orders', fn (OutgoingRecord $r) => $r->key === 'order-o-7'
            && $r->payload === ['id' => 'o-7', 'shipped' => true]
            && $r->headers === ['tenant' => 'acme']
            && $r->eventType === OrderShipped::class);
    }

    public function test_after_commit_is_the_default_and_rollbacks_drop_the_record(): void
    {
        Samsa::fake();

        try {
            DB::transaction(function () {
                OrderPlaced::dispatch('o-1', 1.0);

                throw new RuntimeException('rollback');
            });
        } catch (RuntimeException) {
        }

        Samsa::assertNotProduced('orders');
        Samsa::assertProduced('analytics'); // afterCommit: false on that attribute

        DB::transaction(fn () => OrderPlaced::dispatch('o-2', 1.0));
        Samsa::assertProducedWithKey('orders', 'o-2');
    }

    public function test_events_are_written_for_real_without_the_fake(): void
    {
        Samsa::createTopic('orders', 2);
        Samsa::createTopic('analytics', 1);

        OrderPlaced::dispatch('o-1', 10.0);

        $this->assertSame(1, Samsa::stats()->lag('g', 'orders'));
        $this->assertSame(1, Samsa::stats()->lag('g', 'analytics'));
    }

    public function test_publish_with_and_without_the_attribute(): void
    {
        Samsa::fake();

        Samsa::publish(new OrderShipped('o-3'));
        Samsa::publish(new NotPublished('v'), topic: 'misc', key: 'k');

        Samsa::assertProducedWithKey('orders', 'order-o-3');
        Samsa::assertProduced('misc', fn (OutgoingRecord $r) => $r->key === 'k' && $r->payload === ['value' => 'v']);

        $this->expectException(InvalidArgumentException::class);
        Samsa::publish(new NotPublished);
    }

    public function test_broadcast_events_can_be_forwarded(): void
    {
        config(['samsa.bridge.forward_broadcast_events' => true]);
        $this->app->forgetInstance(EventPublisher::class);
        Samsa::fake();

        event(new ChatMessageSent('lobby', 'hi'));

        Samsa::assertProduced('broadcasts', fn (OutgoingRecord $r) => $r->key === 'chat.lobby'
            && $r->eventType === 'chat.sent'
            && $r->payload === ['room' => 'lobby', 'text' => 'hi']);
    }

    public function test_broadcast_events_are_not_forwarded_by_default(): void
    {
        Samsa::fake();

        event(new ChatMessageSent('lobby', 'hi'));

        Samsa::assertNothingProduced();
    }

    public function test_unsupported_values_explain_how_to_fix_them(): void
    {
        $this->expectException(SerializationException::class);
        $this->expectExceptionMessage('ToSamsaPayload');

        (new PayloadNormalizer)->normalize(fopen('php://memory', 'r'));
    }
}
