<?php

namespace Siberfx\Samsa\Tests\Fixtures;

use DateTimeImmutable;
use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Broadcasting\ShouldBroadcast;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable as DispatchableJob;
use Illuminate\Foundation\Events\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use RuntimeException;
use Siberfx\Samsa\Attributes\PublishTo;
use Siberfx\Samsa\Contracts\ToSamsaPayload;
use Throwable;

enum OrderStatus: string
{
    case Placed = 'placed';
    case Paid = 'paid';
}

#[PublishTo('orders', key: 'orderId', eventType: 'order.placed')]
#[PublishTo('analytics', eventType: 'order.placed', afterCommit: false)]
final class OrderPlaced
{
    use Dispatchable;

    public function __construct(
        public string $orderId,
        public float $total,
        public OrderStatus $status = OrderStatus::Placed,
        public ?DateTimeImmutable $placedAt = null,
    ) {}
}

#[PublishTo('orders', key: 'partitionKey', afterCommit: false)]
final class OrderShipped implements ToSamsaPayload
{
    use Dispatchable;

    public function __construct(public string $orderId) {}

    public function partitionKey(): string
    {
        return "order-{$this->orderId}";
    }

    public function toSamsaPayload(): array
    {
        return ['id' => $this->orderId, 'shipped' => true];
    }

    /**
     * @return array<string, string>
     */
    public function samsaHeaders(): array
    {
        return ['tenant' => 'acme'];
    }
}

final class NotPublished
{
    use Dispatchable;

    public function __construct(public string $value = 'x') {}
}

final class ChatMessageSent implements ShouldBroadcast
{
    public function __construct(public string $room, public string $text) {}

    /**
     * @return array<int, string>
     */
    public function broadcastOn(): array
    {
        return ["chat.{$this->room}"];
    }

    public function broadcastAs(): string
    {
        return 'chat.sent';
    }
}

final class RecordingJob implements ShouldQueue
{
    use DispatchableJob, InteractsWithQueue, Queueable;

    public function __construct(public int $n, public ?string $key = null) {}

    public function samsaKey(): ?string
    {
        return $this->key;
    }

    public function handle(): void
    {
        Journal::$attempts++;
        Journal::$jobs[] = $this->n;
    }
}

final class ReleasingJob implements ShouldQueue
{
    use DispatchableJob, InteractsWithQueue, Queueable;

    public int $tries = 3;

    public function __construct(public int $releaseDelay = 0) {}

    public function handle(): void
    {
        Journal::$attempts++;

        if ($this->attempts() === 1) {
            $this->release($this->releaseDelay);

            return;
        }

        Journal::$jobs[] = $this->attempts();
    }
}

final class FailingJob implements ShouldQueue
{
    use DispatchableJob, InteractsWithQueue, Queueable;

    public int $tries = 2;

    public function handle(): void
    {
        Journal::$attempts++;

        throw new RuntimeException('always fails');
    }

    public function failed(Throwable $e): void
    {
        Journal::$jobs[] = 'failed: '.$e->getMessage();
    }
}
