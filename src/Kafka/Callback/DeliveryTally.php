<?php

declare(strict_types=1);

namespace Workshop\Kafka\Callback;

use RdKafka\Conf;
use RdKafka\Message;
use Workshop\Kafka\Runtime\Narrating;

/**
 * A counting delivery-report callback. Where DeliveryReportCallback only
 * narrates, this one keeps score — the outbox relay flushes a batch and then
 * asks failed() before it dares mark the rows published, so "delivered" is the
 * broker's word, not the producer queue's. reset() starts a new batch.
 */
final class DeliveryTally implements ConfCallback
{
    use Narrating;

    private int $delivered = 0;

    private int $failed = 0;

    public function __construct(
        private readonly ?\Closure $narrate = null,
    ) {
    }

    public function attachTo(Conf $conf): void
    {
        $conf->setDrMsgCb(function ($producer, Message $message): void {
            $this->record($message);
        });
    }

    public function record(Message $message): void
    {
        if (RD_KAFKA_RESP_ERR_NO_ERROR !== $message->err) {
            ++$this->failed;
            $this->narrate(sprintf('✗ delivery failed: %s', $message->errstr()));

            return;
        }

        ++$this->delivered;
        $this->narrate(sprintf('✓ delivered partition=%d offset=%d', $message->partition, $message->offset));
    }

    public function delivered(): int
    {
        return $this->delivered;
    }

    public function failed(): int
    {
        return $this->failed;
    }

    public function reset(): void
    {
        $this->delivered = 0;
        $this->failed = 0;
    }
}
