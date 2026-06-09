<?php

declare(strict_types=1);

namespace Workshop\Kafka\Callback;

use RdKafka\Conf;
use RdKafka\Message;
use Workshop\Kafka\Runtime\Narrating;

/**
 * The producer side of the callback set: librdkafka invokes the delivery-report
 * callback once per message after the broker acks (or fails) it. With an
 * idempotent producer this is how you observe that a send actually landed —
 * narrated in verbose mode, silent otherwise.
 */
final readonly class DeliveryReportCallback implements ConfCallback
{
    use Narrating;

    public function __construct(
        private ?\Closure $narrate = null,
    ) {
    }

    public function attachTo(Conf $conf): void
    {
        $conf->setDrMsgCb(function ($producer, Message $message): void {
            if (RD_KAFKA_RESP_ERR_NO_ERROR !== $message->err) {
                $this->narrate(sprintf('✗ delivery failed: %s', $message->errstr()));

                return;
            }

            $this->narrate(sprintf('✓ delivered partition=%d offset=%d', $message->partition, $message->offset));
        });
    }
}
