<?php

namespace Siberfx\Samsa\Consumer;

use ReflectionClass;
use Siberfx\Samsa\Attributes\Retry;
use Siberfx\Samsa\Exceptions\UnrecoverableRecordException;
use Throwable;

final readonly class RetryPolicy
{
    /**
     * @param  list<int>  $backoffMs  wait before attempt 2, 3, …; the last value repeats
     * @param  list<class-string<Throwable>>  $dontRetry
     */
    public function __construct(
        public int $maxAttempts = 3,
        public array $backoffMs = [1000, 5000, 15000],
        public array $dontRetry = [],
    ) {}

    /**
     * @param  array{max_attempts?: int, backoff_ms?: list<int>, dont_retry?: list<class-string<Throwable>>}  $config
     */
    public static function fromConfig(array $config): self
    {
        return new self(
            max(1, (int) ($config['max_attempts'] ?? 3)),
            array_values(array_map('intval', $config['backoff_ms'] ?? [1000, 5000, 15000])),
            array_values($config['dont_retry'] ?? []),
        );
    }

    /**
     * Apply a #[Retry] attribute declared on the handler's class, if any.
     */
    public function forHandler(object $handler): self
    {
        $attributes = (new ReflectionClass($handler))->getAttributes(Retry::class);

        if ($attributes === []) {
            return $this;
        }

        $retry = $attributes[0]->newInstance();

        return new self(
            max(1, $retry->maxAttempts ?? $this->maxAttempts),
            $retry->backoffMs ?? $this->backoffMs,
            $this->dontRetry,
        );
    }

    public function shouldRetry(Throwable $e, int $attempt): bool
    {
        if ($attempt >= $this->maxAttempts || $e instanceof UnrecoverableRecordException) {
            return false;
        }

        foreach ($this->dontRetry as $class) {
            if ($e instanceof $class) {
                return false;
            }
        }

        return true;
    }

    /**
     * Milliseconds to wait after failed attempt $attempt (1-based).
     */
    public function backoffMs(int $attempt): int
    {
        if ($this->backoffMs === []) {
            return 0;
        }

        return $this->backoffMs[min($attempt, count($this->backoffMs)) - 1];
    }
}
