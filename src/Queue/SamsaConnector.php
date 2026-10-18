<?php

namespace Siberfx\Samsa\Queue;

use Illuminate\Container\Container;
use Illuminate\Contracts\Events\Dispatcher;
use Illuminate\Contracts\Queue\Queue;
use Illuminate\Queue\Connectors\ConnectorInterface;
use Illuminate\Queue\Events\WorkerStopping;

final class SamsaConnector implements ConnectorInterface
{
    public function __construct(private readonly Container $container) {}

    /**
     * @param  array<string, mixed>  $config
     */
    public function connect(array $config): Queue
    {
        /** @var array{topic?: string, group?: string, samsa_driver?: ?string, lease_seconds?: int} $config */
        $queue = new SamsaQueue($this->container, $config);

        // Hand partitions over immediately when queue:work stops gracefully.
        if ($this->container->bound(Dispatcher::class)) {
            $this->container->make(Dispatcher::class)->listen(WorkerStopping::class, fn () => $queue->releaseLeases());
        }

        return $queue;
    }
}
