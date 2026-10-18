<?php

namespace Siberfx\Samsa\Exceptions;

/**
 * Throw from a handler to skip retries and dead-letter the record immediately.
 */
class UnrecoverableRecordException extends SamsaException {}
