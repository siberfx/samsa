<?php

namespace Siberfx\Samsa\Console;

use Illuminate\Console\Command;
use JsonException;
use Siberfx\Samsa\Console\Concerns\UsesSamsaDriver;

class ProduceCommand extends Command
{
    use UsesSamsaDriver;

    protected $signature = 'samsa:produce
        {topic : The topic}
        {payload : The payload as a JSON object}
        {--key= : The record key}
        {--header=* : Headers as name:value}
        {--type= : The event type}
        {--driver= : The Samsa driver to use}';

    protected $description = 'Produce one record (for testing and operations)';

    public function handle(): int
    {
        try {
            $payload = json_decode((string) $this->argument('payload'), true, 512, JSON_THROW_ON_ERROR);
        } catch (JsonException $e) {
            $this->components->error("The payload is not valid JSON: {$e->getMessage()}");

            return self::FAILURE;
        }

        if (! is_array($payload)) {
            $this->components->error('The payload must be a JSON object or array.');

            return self::FAILURE;
        }

        $headers = [];

        foreach ((array) $this->option('header') as $header) {
            [$name, $value] = array_pad(explode(':', (string) $header, 2), 2, '');
            $headers[trim($name)] = trim($value);
        }

        $meta = $this->client()->produce(
            (string) $this->argument('topic'),
            $payload,
            $this->option('key') ?: null,
            $headers,
            eventType: $this->option('type') ?: null,
        );

        $this->components->info("Produced to {$meta->topic}, partition {$meta->partition}, offset {$meta->offset}.");

        return self::SUCCESS;
    }
}
