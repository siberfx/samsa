<?php

namespace Siberfx\Samsa\Contracts;

/**
 * Control the payload of an event produced by the event bridge. An event may
 * also define `samsaHeaders(): array<string, scalar|null>` for extra headers.
 */
interface ToSamsaPayload
{
    /**
     * @return array<string, mixed> JSON-serializable data
     */
    public function toSamsaPayload(): array;
}
