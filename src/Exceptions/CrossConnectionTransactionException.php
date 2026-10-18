<?php

namespace Siberfx\Samsa\Exceptions;

class CrossConnectionTransactionException extends SamsaException
{
    public static function make(string $samsaConnection, string $transactionConnection): self
    {
        return new self(
            "A transaction is open on connection [{$transactionConnection}], but Samsa writes to [{$samsaConnection}], so the record would not be part of it. "
            .'Produce with Samsa::afterCommit(), or point SAMSA_DB_CONNECTION at the same connection.'
        );
    }
}
