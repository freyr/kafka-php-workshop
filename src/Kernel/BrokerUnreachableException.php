<?php

declare(strict_types=1);

namespace Workshop\Kernel;

final class BrokerUnreachableException extends \RuntimeException
{
    public function __construct(string $brokers, string $reason)
    {
        parent::__construct(sprintf(
            "Could not reach Kafka broker at %s (%s).\n"
            . "Did you forget to start the stack? Bring it up with:\n"
            . "    make create\n"
            . '    # or: docker compose up -d',
            $brokers,
            '' !== $reason ? $reason : 'connection refused',
        ));
    }
}
