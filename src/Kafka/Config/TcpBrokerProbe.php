<?php

declare(strict_types=1);

namespace Workshop\Kafka\Config;

use Workshop\Kernel\BrokerUnreachableException;

/**
 * Quick TCP probe of the first broker (ported from the original enqueue-based layer)
 * so a stopped stack surfaces "did you forget to start the broker?" rather than a
 * 90-second librdkafka transport timeout.
 */
final readonly class TcpBrokerProbe implements BrokerProbe
{
    public function __construct(
        private float $timeoutSeconds = 2.0,
    ) {
    }

    public function assertReachable(string $brokers): void
    {
        $first = explode(',', $brokers)[0];
        if (! str_contains($first, ':')) {
            return; // unusual format — let librdkafka handle it
        }

        [$host, $port] = explode(':', $first, 2);
        $errno = 0;
        $errstr = '';
        $sock = @fsockopen($host, (int) $port, $errno, $errstr, $this->timeoutSeconds);

        if (false === $sock) {
            throw new BrokerUnreachableException($brokers, $errstr);
        }

        fclose($sock);
    }
}
