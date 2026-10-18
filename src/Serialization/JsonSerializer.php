<?php

namespace Siberfx\Samsa\Serialization;

use JsonException;
use Siberfx\Samsa\Contracts\Serializer;
use Siberfx\Samsa\Exceptions\SerializationException;

final class JsonSerializer implements Serializer
{
    public function encode(array $data): string
    {
        try {
            return json_encode($data, JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_PRESERVE_ZERO_FRACTION);
        } catch (JsonException $e) {
            throw new SerializationException("Unable to encode record data as JSON: {$e->getMessage()}", 0, $e);
        }
    }

    public function decode(string $data): array
    {
        try {
            $decoded = json_decode($data, true, 512, JSON_THROW_ON_ERROR);
        } catch (JsonException $e) {
            throw new SerializationException("Unable to decode record data: {$e->getMessage()}", 0, $e);
        }

        if (! is_array($decoded)) {
            throw new SerializationException('Decoded record data is not an array.');
        }

        return $decoded;
    }
}
