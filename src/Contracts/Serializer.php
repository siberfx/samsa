<?php

namespace Siberfx\Samsa\Contracts;

use Siberfx\Samsa\Exceptions\SerializationException;

interface Serializer
{
    /**
     * @param  array<array-key, mixed>  $data
     *
     * @throws SerializationException
     */
    public function encode(array $data): string;

    /**
     * @return array<array-key, mixed>
     *
     * @throws SerializationException
     */
    public function decode(string $data): array;
}
