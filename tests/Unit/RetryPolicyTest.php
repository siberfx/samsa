<?php

namespace Siberfx\Samsa\Tests\Unit;

use DomainException;
use InvalidArgumentException;
use LogicException;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Siberfx\Samsa\Attributes\Retry;
use Siberfx\Samsa\Consumer\RetryPolicy;
use Siberfx\Samsa\Contracts\Handler;
use Siberfx\Samsa\Data\Record;
use Siberfx\Samsa\Exceptions\UnrecoverableRecordException;

final class RetryPolicyTest extends TestCase
{
    public function test_retries_until_max_attempts(): void
    {
        $policy = new RetryPolicy(maxAttempts: 3);
        $e = new RuntimeException;

        $this->assertTrue($policy->shouldRetry($e, 1));
        $this->assertTrue($policy->shouldRetry($e, 2));
        $this->assertFalse($policy->shouldRetry($e, 3));
    }

    public function test_unrecoverable_and_dont_retry_exceptions_are_not_retried(): void
    {
        $policy = new RetryPolicy(maxAttempts: 5, dontRetry: [LogicException::class]);

        $this->assertFalse($policy->shouldRetry(new UnrecoverableRecordException, 1));
        $this->assertFalse($policy->shouldRetry(new DomainException, 1), 'subclasses of dont_retry entries match');
        $this->assertTrue($policy->shouldRetry(new RuntimeException, 1));
    }

    public function test_backoff_uses_the_last_value_for_later_attempts(): void
    {
        $policy = new RetryPolicy(backoffMs: [100, 500]);

        $this->assertSame(100, $policy->backoffMs(1));
        $this->assertSame(500, $policy->backoffMs(2));
        $this->assertSame(500, $policy->backoffMs(7));
        $this->assertSame(0, (new RetryPolicy(backoffMs: []))->backoffMs(1));
    }

    public function test_from_config(): void
    {
        $policy = RetryPolicy::fromConfig(['max_attempts' => 0, 'backoff_ms' => ['10'], 'dont_retry' => [InvalidArgumentException::class]]);

        $this->assertSame(1, $policy->maxAttempts, 'at least one attempt');
        $this->assertSame([10], $policy->backoffMs);
        $this->assertSame([InvalidArgumentException::class], $policy->dontRetry);
    }

    public function test_the_retry_attribute_overrides_the_defaults(): void
    {
        $handler = new #[Retry(maxAttempts: 7, backoffMs: [1])] class implements Handler
        {
            public function handle(Record $record): void {}
        };

        $plain = new class implements Handler
        {
            public function handle(Record $record): void {}
        };

        $defaults = new RetryPolicy(3, [1000], [LogicException::class]);

        $this->assertSame(7, $defaults->forHandler($handler)->maxAttempts);
        $this->assertSame([1], $defaults->forHandler($handler)->backoffMs);
        $this->assertSame([LogicException::class], $defaults->forHandler($handler)->dontRetry);
        $this->assertSame($defaults, $defaults->forHandler($plain));
    }
}
