<?php

namespace Siberfx\Samsa\Models;

use Illuminate\Database\Eloquent\Model;
use Illuminate\Support\Carbon;
use LogicException;

/**
 * Read-only view of the database driver's log, for debugging and admin screens.
 * Writing to samsa_* tables directly would bypass offset allocation and fencing.
 *
 * @property int $id
 * @property string $topic
 * @property int $partition
 * @property int $offset
 * @property string|null $key
 * @property string|null $dedup_key
 * @property string|null $event_type
 * @property array<string, mixed> $payload
 * @property array<string, mixed>|null $headers
 * @property Carbon $created_at
 */
class LogEntry extends Model
{
    public $timestamps = false;

    protected $guarded = ['*'];

    public function getConnectionName(): ?string
    {
        return config('samsa.drivers.database.connection') ?? parent::getConnectionName();
    }

    public function getTable(): string
    {
        return config('samsa.drivers.database.table_prefix', '').'samsa_log';
    }

    protected function casts(): array
    {
        return [
            'partition' => 'integer',
            'offset' => 'integer',
            'payload' => 'array',
            'headers' => 'array',
            'created_at' => 'datetime',
        ];
    }

    public function save(array $options = []): bool
    {
        throw new LogicException('LogEntry is read-only. Produce records with Samsa::produce().');
    }

    public function delete(): ?bool
    {
        throw new LogicException('LogEntry is read-only. Use retention (samsa:prune) to remove records.');
    }
}
