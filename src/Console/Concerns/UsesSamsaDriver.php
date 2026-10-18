<?php

namespace Siberfx\Samsa\Console\Concerns;

use Siberfx\Samsa\Client;
use Siberfx\Samsa\SamsaManager;

trait UsesSamsaDriver
{
    protected function samsa(): SamsaManager
    {
        return $this->laravel->make('samsa');
    }

    protected function client(): Client
    {
        return $this->samsa()->driver($this->driverName());
    }

    protected function driverName(): ?string
    {
        $driver = $this->option('driver');

        return is_string($driver) && $driver !== '' ? $driver : null;
    }
}
