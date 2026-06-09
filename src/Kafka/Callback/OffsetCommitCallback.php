<?php

declare(strict_types=1);

namespace Workshop\Kafka\Callback;

use RdKafka\Conf;
use RdKafka\KafkaConsumer;
use RdKafka\TopicPartition;
use Workshop\Kafka\Runtime\Narrating;
use Workshop\Kafka\Runtime\RebalanceCommitError;

/**
 * Reports the result of asynchronous offset commits.
 *
 * Under the async commit policy the run-loop fires commitAsync() and moves on, so a
 * commit the broker later rejects never surfaces as an exception the loop can catch.
 * librdkafka's fallback for an unhandled commit result is to log a raw
 * `%4|...COMMITFAIL...` warning from its background thread; registering this callback
 * both silences that line (librdkafka only logs when no callback is set) and lets the
 * rejection be narrated in the run's own voice.
 *
 * Rejections caused by a rebalance — the partition was revoked, or the group
 * generation advanced, between the handler and the commit landing — are expected and
 * benign: the record is redelivered to the partition's new owner, so at-least-once
 * still holds. They are narrated as a skip, not an error. Any other commit failure is
 * a genuine fault and is narrated as one.
 */
final readonly class OffsetCommitCallback implements ConfCallback
{
    use Narrating;

    public function __construct(
        private ?\Closure $narrate = null,
    ) {
    }

    public function attachTo(Conf $conf): void
    {
        $conf->setOffsetCommitCb(
            /** @param array<int, TopicPartition>|null $partitions */
            function (KafkaConsumer $consumer, int $err, ?array $partitions = null): void {
                /** @var array<int, TopicPartition> $committed */
                $committed = $partitions ?? [];
                $line = $this->describe($err, $committed);
                if (null !== $line) {
                    $this->narrate($line);
                }
            });
    }

    /**
     * The narration a commit result warrants, or null when it warrants none. A
     * successful commit — or one with nothing to commit — is silent; the run-loop
     * already logged the enqueue. A rebalance-class rejection is a benign skip; any
     * other error is a genuine failure. Pure and public so the branching can be
     * exercised without a live broker.
     *
     * @param array<int, TopicPartition> $partitions
     */
    public function describe(int $err, array $partitions): ?string
    {
        if (RD_KAFKA_RESP_ERR_NO_ERROR === $err || RD_KAFKA_RESP_ERR__NO_OFFSET === $err) {
            return null;
        }

        $where = $this->names($partitions);
        if (RebalanceCommitError::matches($err)) {
            return sprintf('⚠ async commit skipped — %s reassigned mid-rebalance; will be redelivered', $where);
        }

        return sprintf('⚠ async commit failed (%s): %s', rd_kafka_err2str($err), $where);
    }

    /**
     * @param array<int, TopicPartition> $partitions
     */
    private function names(array $partitions): string
    {
        $names = array_map(
            static fn (TopicPartition $p): string => sprintf('%s[%d]@%d', $p->getTopic(), $p->getPartition(), $p->getOffset()),
            $partitions,
        );

        return implode(', ', $names) ?: '(none)';
    }
}
