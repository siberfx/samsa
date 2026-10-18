<?php

namespace Siberfx\Samsa\Tests\Unit;

use InvalidArgumentException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Siberfx\Samsa\Consumer\WorkerOptions;
use Siberfx\Samsa\Data\RetentionPolicy;
use Siberfx\Samsa\Data\TopicConfig;
use Siberfx\Samsa\Exceptions\InvalidTopicException;
use Siberfx\Samsa\Exceptions\SerializationException;
use Siberfx\Samsa\Serialization\JsonSerializer;
use Siberfx\Samsa\Testing\RecordFactory;

final class DataTest extends TestCase
{
    /**
     * @return array<string, array{string}>
     */
    public static function invalidTopicNames(): array
    {
        return [
            'empty' => [''],
            'uppercase' => ['Orders'],
            'space' => ['my orders'],
            'slash' => ['orders/eu'],
        ];
    }

    #[DataProvider('invalidTopicNames')]
    public function test_topic_names_are_validated(string $name): void
    {
        $this->expectException(InvalidTopicException::class);

        new TopicConfig($name);
    }

    public function test_valid_topic_names_and_partition_bounds(): void
    {
        $this->assertSame('orders.eu-west_1', (new TopicConfig('orders.eu-west_1', 1))->name);

        $this->expectException(InvalidTopicException::class);
        new TopicConfig('orders', 0);
    }

    public function test_retention_round_trips_through_arrays(): void
    {
        $policy = RetentionPolicy::fromArray(['max_age_seconds' => '60', 'respect_slowest_consumer' => 1]);

        $this->assertSame(['max_age_seconds' => 60, 'max_records_per_partition' => null, 'respect_slowest_consumer' => true], $policy->toArray());
    }

    public function test_json_serializer_round_trip_and_errors(): void
    {
        $serializer = new JsonSerializer;
        $data = ['a' => 1.0, 'b' => 'ünïcode/slash', 'c' => [1, null]];

        $this->assertSame($data, $serializer->decode($serializer->encode($data)));

        try {
            $serializer->encode(['bad' => NAN]);
            $this->fail('NAN must not encode');
        } catch (SerializationException) {
        }

        $this->expectException(SerializationException::class);
        $serializer->decode('"just a string"');
    }

    public function test_record_helpers(): void
    {
        $record = RecordFactory::make('orders', ['n' => 1], key: 'k', headers: ['trace_id' => 'abc']);

        $this->assertSame('abc', $record->header('trace_id'));
        $this->assertSame('fallback', $record->header('missing', 'fallback'));
        $this->assertNotNull($record->messageId());
        $this->assertFalse($record->isRedelivery());
        $this->assertTrue($record->withDeliveryAttempt(2)->isRedelivery());
        $this->assertSame($record->messageId(), $record->withDeliveryAttempt(2)->messageId());
    }

    public function test_worker_options_validation(): void
    {
        $options = WorkerOptions::fromConfig(['batch_size' => 10, 'lease_seconds' => 20, 'heartbeat_seconds' => 5], ['commit' => 'record', 'once' => true]);

        $this->assertSame(10, $options->batchSize);
        $this->assertSame('record', $options->commit);
        $this->assertTrue($options->once);

        $this->expectException(InvalidArgumentException::class);
        new WorkerOptions(leaseSeconds: 10, heartbeatSeconds: 8);
    }
}
