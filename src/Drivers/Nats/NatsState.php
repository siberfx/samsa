<?php

namespace Siberfx\Samsa\Drivers\Nats;

/**
 * A small key-value store on a JetStream stream that keeps one message per
 * subject: offsets, leases, members, topic metadata, dead letters and delayed
 * records. Writes can be compare-and-swap on the subject's last sequence.
 *
 * @internal
 */
final class NatsState
{
    private bool $ensured = false;

    public function __construct(private readonly NatsConnection $nats) {}

    /**
     * @return array{value: array<string, mixed>, revision: int}|null
     */
    public function get(string $key): ?array
    {
        $this->ensure();

        $message = $this->nats->getMessage($this->stream(), ['last_by_subj' => $this->subject($key)]);

        if ($message === null) {
            return null;
        }

        return ['value' => (array) json_decode($message['data'], true), 'revision' => $message['seq']];
    }

    /**
     * @param  array<string, mixed>  $value
     */
    public function put(string $key, array $value): void
    {
        $this->ensure();
        $this->nats->publish($this->subject($key), (string) json_encode($value));
    }

    /**
     * Write only if the key doesn't exist yet.
     *
     * @param  array<string, mixed>  $value
     */
    public function create(string $key, array $value): bool
    {
        return $this->update($key, $value, 0);
    }

    /**
     * Write only if the key is still at $revision.
     *
     * @param  array<string, mixed>  $value
     */
    public function update(string $key, array $value, int $revision): bool
    {
        $this->ensure();

        return $this->nats->publish($this->subject($key), (string) json_encode($value), [
            'Nats-Expected-Last-Subject-Sequence' => (string) $revision,
        ]) !== null;
    }

    public function delete(string $key): void
    {
        $entry = $this->get($key);

        if ($entry !== null) {
            $this->nats->api("STREAM.MSG.DELETE.{$this->stream()}", ['seq' => $entry['revision'], 'no_erase' => true], [NatsConnection::ERR_NO_MESSAGE]);
        }
    }

    /**
     * Keys matching a subject-style pattern ("offsets.*.orders.*", "dlq.>").
     *
     * @return list<string>
     */
    public function keys(string $pattern): array
    {
        $this->ensure();

        $info = $this->nats->api("STREAM.INFO.{$this->stream()}", ['subjects_filter' => $this->subject($pattern)]);
        $subjects = (array) ($info->state->subjects ?? []);
        $prefixLength = strlen($this->subject(''));

        $keys = array_map(fn ($subject) => substr((string) $subject, $prefixLength), array_keys($subjects));
        sort($keys);

        return $keys;
    }

    public function stream(): string
    {
        return strtoupper($this->nats->prefix()).'__STATE';
    }

    private function subject(string $key): string
    {
        return $this->nats->prefix().'-state.'.$key;
    }

    private function ensure(): void
    {
        if ($this->ensured) {
            return;
        }

        $this->nats->ensureStream([
            'name' => $this->stream(),
            'subjects' => [$this->nats->prefix().'-state.>'],
            'retention' => 'limits',
            'storage' => $this->nats->storage(),
            'num_replicas' => $this->nats->replicas(),
            'max_msgs_per_subject' => 1,
            'discard' => 'old',
            'allow_direct' => true,
        ]);

        $this->ensured = true;
    }
}
