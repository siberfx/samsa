<?php

namespace Siberfx\Samsa\Drivers\Nats;

use Basis\Nats\Client;
use Basis\Nats\Configuration;
use Basis\Nats\Message\Payload;
use Exception;
use Siberfx\Samsa\Exceptions\DriverException;
use Throwable;

/**
 * Thin JetStream API layer over basis-company/nats: JSON requests to $JS.API.*,
 * publishes that wait for the stream's PubAck, and naming for streams/subjects.
 *
 * @internal
 */
final class NatsConnection
{
    public const ERR_NO_MESSAGE = 10037;

    public const ERR_STREAM_NOT_FOUND = 10059;

    public const ERR_STREAM_NAME_IN_USE = 10058;

    public const ERR_WRONG_LAST_SEQUENCE = 10071;

    private ?Client $client = null;

    /**
     * @param  array{host?: string, port?: int, user?: ?string, pass?: ?string, token?: ?string, timeout?: float, reconnect?: bool, storage?: string, replicas?: int, prefix?: string, duplicate_window?: int}  $config
     */
    public function __construct(private readonly array $config = []) {}

    public function prefix(): string
    {
        return (string) ($this->config['prefix'] ?? 'samsa');
    }

    public function storage(): string
    {
        return (string) ($this->config['storage'] ?? 'file');
    }

    public function replicas(): int
    {
        return (int) ($this->config['replicas'] ?? 1);
    }

    public function duplicateWindowNanos(): int
    {
        return (int) ($this->config['duplicate_window'] ?? 120) * 1_000_000_000;
    }

    public function topicStream(string $topic): string
    {
        return strtoupper($this->prefix()).'_'.self::token($topic);
    }

    public function partitionSubject(string $topic, int $partition): string
    {
        return $this->prefix().'.'.self::token($topic).'.'.$partition;
    }

    public function topicSubjects(string $topic): string
    {
        return $this->prefix().'.'.self::token($topic).'.*';
    }

    /**
     * A JetStream API call. Returns null for "not found" errors listed in $notFound.
     *
     * @param  array<string, mixed>  $args
     * @param  list<int>  $notFound
     */
    public function api(string $command, array $args = [], array $notFound = []): ?object
    {
        try {
            return $this->client()->api($command, $args);
        } catch (Exception $e) {
            if (in_array((int) $e->getCode(), $notFound, true)) {
                return null;
            }

            throw DriverException::wrap('nats', $e);
        }
    }

    /**
     * Publish to a stream subject and wait for its PubAck.
     *
     * @param  array<string, string>  $headers
     * @return object|null the PubAck (stream, seq, duplicate), or null when an
     *                     expected-sequence header did not match
     */
    public function publish(string $subject, string $body, array $headers = []): ?object
    {
        try {
            /** @var Payload $ack */
            $ack = $this->client()->dispatch($subject, new Payload($body, $headers));
        } catch (Throwable $e) {
            throw DriverException::wrap('nats', $e);
        }

        /** @var array{stream?: string, seq?: int, duplicate?: bool, error?: array{err_code?: int, description?: string}} $data */
        $data = (array) json_decode($ack->body, true);

        if (isset($data['error'])) {
            if ((int) ($data['error']['err_code'] ?? 0) === self::ERR_WRONG_LAST_SEQUENCE) {
                return null;
            }

            throw new DriverException('NATS rejected the publish: '.($data['error']['description'] ?? 'unknown error'));
        }

        return (object) ['stream' => $data['stream'] ?? null, 'seq' => (int) ($data['seq'] ?? 0), 'duplicate' => (bool) ($data['duplicate'] ?? false)];
    }

    /**
     * STREAM.MSG.GET, decoded. Returns null when no message matches.
     *
     * @param  array<string, mixed>  $query  seq / next_by_subj / last_by_subj
     * @return array{seq: int, subject: string, headers: array<string, string>, data: string}|null
     */
    public function getMessage(string $stream, array $query): ?array
    {
        $response = $this->api("STREAM.MSG.GET.{$stream}", $query, [self::ERR_NO_MESSAGE, self::ERR_STREAM_NOT_FOUND]);
        $message = $response === null ? null : ($response->message ?? null);

        if ($message === null) {
            return null;
        }

        return [
            'seq' => (int) $message->seq,
            'subject' => (string) $message->subject,
            'headers' => self::parseHeaders(isset($message->hdrs) ? (string) base64_decode((string) $message->hdrs) : ''),
            'data' => isset($message->data) ? (string) base64_decode((string) $message->data) : '',
        ];
    }

    /**
     * Ensure a stream exists with the given config (created, or left as is).
     *
     * @param  array<string, mixed>  $config
     */
    public function ensureStream(array $config): void
    {
        $this->api("STREAM.CREATE.{$config['name']}", $config, [self::ERR_STREAM_NAME_IN_USE]);
    }

    public function disconnect(): void
    {
        $this->client?->disconnect();
        $this->client = null;
    }

    public function client(): Client
    {
        return $this->client ??= new Client(new Configuration(
            host: (string) ($this->config['host'] ?? '127.0.0.1'),
            port: (int) ($this->config['port'] ?? 4222),
            user: $this->config['user'] ?? null,
            pass: $this->config['pass'] ?? null,
            token: $this->config['token'] ?? null,
            reconnect: (bool) ($this->config['reconnect'] ?? true),
            timeout: (float) ($this->config['timeout'] ?? 1.0),
        ));
    }

    /**
     * Subject tokens can't contain ".", "*", ">" or whitespace: names that aren't
     * plain [A-Za-z0-9_-] are hex-encoded behind a "~".
     */
    public static function token(string $name): string
    {
        return preg_match('/^[A-Za-z0-9_-]+$/', $name) === 1 ? $name : '~'.bin2hex($name);
    }

    public static function untoken(string $token): string
    {
        return str_starts_with($token, '~') ? (string) hex2bin(substr($token, 1)) : $token;
    }

    /**
     * @return array<string, string>
     */
    public static function parseHeaders(string $raw): array
    {
        $headers = [];

        foreach (explode("\r\n", $raw) as $line) {
            if (! str_contains($line, ':')) {
                continue;
            }

            [$name, $value] = explode(':', $line, 2);
            $headers[trim($name)] = trim($value);
        }

        return $headers;
    }
}
