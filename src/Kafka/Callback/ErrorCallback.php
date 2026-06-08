<?php

declare(strict_types=1);

namespace Workshop\Kafka\Callback;

use RdKafka\Conf;

/**
 * Surfaces librdkafka's asynchronous error events (broker down, auth failure,
 * transport errors) through the narration sink instead of letting them vanish.
 */
final readonly class ErrorCallback implements ConfCallback
{
    use Narrating;

    public function __construct(
        private ?\Closure $narrate = null,
    ) {
    }

    public function attachTo(Conf $conf): void
    {
        $conf->setErrorCb(function ($client, int $err, string $reason): void {
            $this->narrate(sprintf('librdkafka error %s: %s', rd_kafka_err2str($err), $reason));
        });
    }
}
