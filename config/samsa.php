<?php

use Siberfx\Samsa\Producer\DefaultPartitioner;
use Siberfx\Samsa\Serialization\JsonSerializer;

return [

    /*
    |--------------------------------------------------------------------------
    | Default driver
    |--------------------------------------------------------------------------
    |
    | Supported: "database", "memory". The "nats" (v1.2) and "redis" (v1.3)
    | drivers are planned.
    |
    */

    'default' => env('SAMSA_DRIVER', 'database'),

    'drivers' => [
        'database' => [
            'connection' => env('SAMSA_DB_CONNECTION'), // null = app default connection
            'table_prefix' => '',
        ],

        'nats' => [
            'host' => env('NATS_HOST', '127.0.0.1'),
            'port' => (int) env('NATS_PORT', 4222),
            'user' => env('NATS_USER'),
            'pass' => env('NATS_PASS'),
            'token' => env('NATS_TOKEN'),
            'timeout' => 1.0,
            'reconnect' => true,
            'storage' => 'file',
            'replicas' => 1,
            'kv_bucket' => 'samsa_kv_leases',
        ],

        'redis' => [
            'connection' => env('SAMSA_REDIS_CONNECTION', 'default'),
            'prefix' => 'samsa:',
        ],

        'memory' => [],
    ],

    /*
    |--------------------------------------------------------------------------
    | Topics
    |--------------------------------------------------------------------------
    */

    'defaults' => [
        'partitions' => 8,
        'retention' => [
            'max_age_seconds' => 60 * 60 * 24 * 7,
            'max_records_per_partition' => null,
            'respect_slowest_consumer' => false,
        ],
    ],

    'topics' => [
        // 'orders' => ['partitions' => 16, 'retention' => ['max_age_seconds' => 2592000]],
    ],

    // Create unknown topics on first produce (handy locally; keep off in production)
    'auto_create_topics' => env('SAMSA_AUTO_CREATE_TOPICS', false),

    /*
    |--------------------------------------------------------------------------
    | Consumers
    |--------------------------------------------------------------------------
    */

    'consumer' => [
        'batch_size' => 100,
        'poll_interval_ms' => 250,
        'lease_seconds' => 30,
        'heartbeat_seconds' => 10,
        'commit' => 'batch',          // batch | record
        'start_from' => 'earliest',   // earliest | latest
        'max_partitions_per_worker' => null,
    ],

    'retry' => [
        'max_attempts' => 3,
        'backoff_ms' => [1000, 5000, 15000],
        'dont_retry' => [],
    ],

    'dead_letter' => [
        'enabled' => true,
    ],

    'schedule' => [
        'prune' => env('SAMSA_SCHEDULE_PRUNE', true),
        // null = run samsa:release-delayed every second when a queue connection uses the samsa driver
        'release_delayed' => env('SAMSA_SCHEDULE_DELAYED'),
    ],

    /*
    |--------------------------------------------------------------------------
    | Event bridge
    |--------------------------------------------------------------------------
    |
    | Events marked with #[PublishTo] are produced when dispatched. Optionally
    | every ShouldBroadcast event is mirrored to `broadcast_topic` as well.
    |
    */

    'bridge' => [
        'enabled' => true,
        'forward_broadcast_events' => false,
        'broadcast_topic' => 'broadcasts',
    ],

    'serializer' => JsonSerializer::class,
    'partitioner' => DefaultPartitioner::class,

    /*
    |--------------------------------------------------------------------------
    | Subscriptions
    |--------------------------------------------------------------------------
    |
    | Consumer group => [topic => handler class]. You may also register them
    | in code with Samsa::subscribe($group, $topic, $handler).
    |
    */

    'subscriptions' => [
        // 'billing' => ['orders' => \App\Samsa\Handlers\ChargeOrder::class],
    ],
];
