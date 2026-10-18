# Testing

Samsa gives you three levels of testing support:

| Tool | Use it to | Speed | Status |
|---|---|---|---|
| [`Samsa::fake()`](#faking-the-producer) | Assert *what* your code produces, without running consumers | Fastest | `v1.1` |
| [`RecordFactory`](#testing-handlers) | Unit-test handlers | Fastest | ✅ |
| [Memory driver](#the-memory-driver) | Run real produce → consume flows in-process | Fast | ✅ |
| [Integration tests](#integration-tests) | Test against real MySQL, PostgreSQL, NATS or Redis | Slower | ✅ (`StorageDriverContract`) |

`Samsa::fake()`, `Samsa::consumeUntilEmpty()`, `Samsa::redeliver()` and `Samsa::assertDeadLettered()` ship in `v1.1`. Until then, use the memory driver with `samsa:consume --stop-when-empty` or `Samsa::worker()->run(...)`.

- [Faking the producer](#faking-the-producer)
- [Testing handlers](#testing-handlers)
- [The memory driver](#the-memory-driver)
- [Running a consumer in a test](#running-a-consumer-in-a-test)
- [Testing redelivery](#testing-redelivery)
- [Testing the queue driver](#testing-the-queue-driver)
- [Integration tests](#integration-tests)

## Faking the producer

```php
use Siberfx\Samsa\Facades\Samsa;
use Siberfx\Samsa\Data\OutgoingRecord;

public function test_placing_an_order_publishes_an_event(): void
{
    Samsa::fake();

    $this->postJson('/orders', ['sku' => 'ABC', 'qty' => 2])->assertCreated();

    Samsa::assertProduced('orders', function (OutgoingRecord $record) {
        return $record->eventType === 'order.placed'
            && $record->payload['sku'] === 'ABC';
    });
    Samsa::assertProducedCount('orders', 1);
    Samsa::assertNotProduced('payments');
}
```

| Assertion | |
|---|---|
| `assertProduced(topic, ?callback)` | At least one matching record |
| `assertProducedTimes(topic, times, ?callback)` | Exactly N matching records |
| `assertProducedCount(topic, count)` | Exactly N records on the topic |
| `assertNotProduced(topic, ?callback)` | No matching record |
| `assertNothingProduced()` | No records at all |
| `assertProducedWithKey(topic, key)` | A record with this key |
| `produced(topic)` | `Collection` of `OutgoingRecord` for custom assertions |

Fake only some topics, letting others go through the real driver:

```php
Samsa::fake(['orders', 'payments']);
```

The fake also captures records from the [event bridge](event-bridge.md) and the [queue driver](queue-driver.md), although for jobs `Queue::fake()` is usually the better tool.

## Testing handlers

Handlers are plain classes, so test them by building a `Record`:

```php
use Siberfx\Samsa\Testing\RecordFactory;

public function test_it_charges_the_order(): void
{
    $gateway = $this->mock(PaymentGateway::class);
    $gateway->expects('charge')->with('42', 99.9);

    $record = RecordFactory::make('orders', ['order_id' => '42', 'total' => 99.9], key: '42');

    app(ChargeOrder::class)->handle($record);
}
```

`RecordFactory::make()` fills in a partition, an offset, `createdAt` and the standard headers. Override any of them with named arguments (`partition:`, `offset:`, `headers:`, `deliveryAttempt:`, `eventType:`).

## The memory driver

The `memory` driver implements the full driver contract (partitions, offsets, groups, leases, dead letters) in PHP arrays. Use it in `phpunit.xml`:

```xml
<env name="SAMSA_DRIVER" value="memory"/>
```

State lives in the application container, so it resets between tests automatically.

## Running a consumer in a test

```php
public function test_orders_flow_through_billing(): void
{
    Samsa::subscribe('billing', 'orders', ChargeOrder::class);

    Samsa::produce('orders', ['order_id' => '42', 'total' => 99.9], key: '42');

    $this->artisan('samsa:consume billing orders --stop-when-empty')->assertSuccessful();

    $this->assertDatabaseHas('payments', ['order_id' => '42']);
    $this->assertSame(0, Samsa::stats()->lag('billing', 'orders'));
}
```

Or without Artisan:

```php
Samsa::consumeUntilEmpty('billing', 'orders');
```

Check dead letters after a failing flow:

```php
Samsa::assertDeadLettered('billing', 'orders', fn (FailedRecord $f) => $f->exception instanceof GatewayDown);
```

## Testing redelivery

Prove that a handler is idempotent by delivering the same record twice:

```php
public function test_loyalty_points_are_credited_once(): void
{
    $record = RecordFactory::make('orders', ['customer_id' => 7, 'points' => 50]);
    $handler = app(CreditLoyaltyPoints::class);

    $handler->handle($record);
    $handler->handle($record);   // simulated redelivery: same message_id

    $this->assertSame(50, Customer::find(7)->points);
}
```

`Samsa::redeliver('billing', 'orders')` seeks the group back to the start of the records it already consumed, which is the end-to-end equivalent.

## Testing the queue driver

```php
public function test_jobs_run_on_samsa(): void
{
    config(['queue.default' => 'samsa']);

    SyncInventory::dispatch(productId: 7, delta: -1);

    $this->artisan('queue:work samsa --stop-when-empty')->assertSuccessful();

    $this->assertSame(9, Product::find(7)->stock);
}
```

## Integration tests

The package's own test suite runs every driver through the same **contract tests**. You can reuse them for a custom driver:

```php
use PHPUnit\Framework\TestCase;
use Siberfx\Samsa\Contracts\StorageDriver;
use Siberfx\Samsa\Testing\StorageDriverContract;

final class MyDriverTest extends TestCase
{
    use StorageDriverContract;

    protected function driver(): StorageDriver
    {
        return new MyDriver(/* ... */);
    }
}
```

The suite moves time with Carbon's test clock (`CarbonImmutable::setTestNow()`) to expire leases and age records, so your driver must take "now" from Carbon, or from a clock that honours it.

The package's own suite also runs multi-process tests (concurrent producers, and workers killed while consuming) against a real server:

```bash
SAMSA_TEST_DB_CONNECTION=mysql DB_DATABASE=samsa_test DB_USERNAME=root vendor/bin/phpunit --testsuite Integration
```

For your own app, run the test suite against the real backend in CI:

```yaml
# .github/workflows/tests.yml (excerpt)
services:
  mysql:
    image: mysql:8.4
    env: { MYSQL_DATABASE: testing, MYSQL_ALLOW_EMPTY_PASSWORD: 'yes' }
    ports: ['3306:3306']
steps:
  - run: docker run -d -p 4222:4222 nats:2.10-alpine --jetstream   # NATS needs args, so it can't be a service
  - run: php artisan test
    env:
      SAMSA_DRIVER: nats
      NATS_HOST: 127.0.0.1
```

> **SQLite caveat:** SQLite serializes all writes, so it can't reproduce lock contention between producers. Run at least one CI job on MySQL or PostgreSQL if you produce from several processes.
