<?php

namespace Siberfx\Samsa\Consumer;

use RuntimeException;

/**
 * @internal Thrown inside the worker to abandon a retry backoff on SIGQUIT.
 */
final class StopImmediately extends RuntimeException {}
