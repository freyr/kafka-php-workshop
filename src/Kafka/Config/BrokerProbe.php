<?php

declare(strict_types=1);

namespace Workshop\Kafka\Config;

/**
 * Fail fast with a friendly message when the broker is unreachable, instead of
 * letting librdkafka time out silently in the background. An interface so the
 * config layer can be exercised in tests without opening a socket.
 */
interface BrokerProbe
{
    public function assertReachable(string $brokers): void;
}
