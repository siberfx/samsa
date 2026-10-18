<?php

namespace Siberfx\Samsa\Exceptions;

class TopicNotFoundException extends SamsaException
{
    public static function named(string $topic): self
    {
        return new self("Topic [{$topic}] does not exist. Create it with `php artisan samsa:topic:create {$topic}` or enable samsa.auto_create_topics.");
    }
}
