<?php

namespace Siberfx\Samsa\Tests\Feature;

use Carbon\CarbonImmutable;
use Siberfx\MessageBus\Envelope;
use Siberfx\MessageBus\MessageBusServiceProvider;
use Siberfx\MessageBus\Stamp\DelayStamp;
use Siberfx\MessageBus\Stamp\TransportMessageIdStamp;
use Siberfx\MessageBus\Transport\Serialization\PhpSerializer;
use Siberfx\MessageBus\Transport\TransportManager;
use Siberfx\Samsa\Bus\HasPartitionKey;
use Siberfx\Samsa\Bus\PartitionKeyStamp;
use Siberfx\Samsa\Bus\SamsaTransport;
use Siberfx\Samsa\Data\OutgoingRecord;
use Siberfx\Samsa\Data\Record;
use Siberfx\Samsa\Facades\Samsa;
use Siberfx\Samsa\Tests\TestCase;

final class BusOrderPlaced implements HasPartitionKey
{
    public function __construct(public string $orderId) {}

    public function partitionKey(): string
    {
        return "order-{$this->orderId}";
    }
}

final class BusPing
{
    public function __construct(public int $n = 0) {}
}

final class MessageBusTransportTest extends TestCase
{
    protected function setUp(): void
    {
        if (! class_exists(TransportManager::class)) {
            $this->markTestSkipped('siberfx/message-bus is not installed (it requires Laravel 13).');
        }

        parent::setUp();

        Samsa::createTopic('bus-events', 2);
    }

    protected function getPackageProviders($app): array
    {
        return class_exists(MessageBusServiceProvider::class)
            ? [...parent::getPackageProviders($app), MessageBusServiceProvider::class]
            : parent::getPackageProviders($app);
    }

    protected function defineEnvironment($app): void
    {
        parent::defineEnvironment($app);

        $app['config']->set('message-bus.transports.events', [
            'driver' => 'samsa',
            'topic' => 'bus-events',
            'group' => 'message-bus',
            'batch_size' => 10,
        ]);
    }

    private function transport(): SamsaTransport
    {
        return new SamsaTransport($this->app, new PhpSerializer, 'bus-events', 'message-bus', null, 10, 30);
    }

    public function test_the_samsa_driver_is_registered_with_message_bus(): void
    {
        $this->assertInstanceOf(SamsaTransport::class, $this->app->make(TransportManager::class)->get('events'));
    }

    public function test_send_get_and_ack(): void
    {
        $transport = $this->transport();

        $sent = $transport->send(Envelope::wrap(new BusOrderPlaced('o-1')));
        $this->assertMatchesRegularExpression('/^\d+:0$/', (string) $sent->last(TransportMessageIdStamp::class)?->id);

        $received = [...$transport->get()];

        $this->assertCount(1, $received);
        $this->assertInstanceOf(BusOrderPlaced::class, $received[0]->getMessage());
        $this->assertSame('o-1', $received[0]->getMessage()->orderId);
        $this->assertSame(1, $transport->getMessageCount());

        $transport->ack($received[0]);

        $this->assertSame(0, $transport->getMessageCount());
        $this->assertSame([], [...$transport->get()]);
    }

    public function test_partition_keys_come_from_the_message_or_a_stamp(): void
    {
        $transport = $this->transport();

        $transport->send(Envelope::wrap(new BusOrderPlaced('o-9')));
        $transport->send(Envelope::wrap(new BusPing, [new PartitionKeyStamp('custom')]));

        $keys = [];

        foreach ([0, 1] as $p) {
            foreach (Samsa::storage()->log()->read('bus-events', $p, null, 10) as $record) {
                $keys[] = $record->key;
                $this->assertSame(['body', 'headers'], array_keys($record->payload));
            }
        }

        sort($keys);
        $this->assertSame(['custom', 'order-o-9'], $keys);
    }

    public function test_commits_only_move_over_finished_records(): void
    {
        Samsa::createTopic('single', 1);
        $transport = new SamsaTransport($this->app, new PhpSerializer, 'single', 'message-bus', null, 10, 30);

        foreach ([1, 2, 3] as $n) {
            $transport->send(Envelope::wrap(new BusPing($n)));
        }

        [$first, $second, $third] = [...$transport->get()];
        $committed = fn () => Samsa::offsets('message-bus', 'single')->committed(0);

        $transport->ack($second);
        $this->assertNull($committed(), 'the first record is still in flight');

        $transport->reject($first);
        $this->assertSame('1', $committed(), 'reject also moves past (message-bus re-sends retries itself)');

        $transport->ack($third);
        $this->assertSame('2', $committed());
    }

    public function test_delay_stamps_go_through_the_delayed_store(): void
    {
        CarbonImmutable::setTestNow('2026-10-19 10:00:00');
        $transport = $this->transport();

        $sent = $transport->send(Envelope::wrap(new BusPing(1), [new DelayStamp(60_000)]));

        $this->assertStringStartsWith('delayed:', (string) $sent->last(TransportMessageIdStamp::class)?->id);
        $this->assertSame([], [...$transport->get()]);

        CarbonImmutable::setTestNow('2026-10-19 10:01:00');
        Samsa::releaseDelayed();
        CarbonImmutable::setTestNow();

        // Same instance: it still leases the partitions it claimed above.
        $received = [...$transport->get()];
        $this->assertCount(1, $received);
        $this->assertSame(1, $received[0]->getMessage()->n);
    }

    public function test_listing_and_finding_pending_messages(): void
    {
        $transport = $this->transport();
        $sent = $transport->send(Envelope::wrap(new BusPing(7)));
        $id = (string) $sent->last(TransportMessageIdStamp::class)?->id;

        $all = [...$transport->all()];
        $this->assertCount(1, $all);
        $this->assertSame(7, $transport->find($id)?->getMessage()->n);
        $this->assertNull($transport->find('nope'));
    }

    public function test_bus_records_are_readable_by_other_groups_and_faked(): void
    {
        $this->transport()->send(Envelope::wrap(new BusPing(3)));

        $types = [];
        Samsa::subscribe('audit', 'bus-events', function (Record $record) use (&$types) {
            $types[] = $record->eventType;
        });
        Samsa::consumeUntilEmpty('audit', 'bus-events');

        $this->assertSame([BusPing::class], $types);

        Samsa::fake();
        $this->transport()->send(Envelope::wrap(new BusPing(4)));
        Samsa::assertProduced('bus-events', fn (OutgoingRecord $r) => $r->eventType === BusPing::class);
    }
}
