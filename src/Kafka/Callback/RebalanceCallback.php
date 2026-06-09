<?php

declare(strict_types=1);

namespace Workshop\Kafka\Callback;

use RdKafka\Conf;
use RdKafka\KafkaConsumer;
use RdKafka\TopicPartition;
use Workshop\Kafka\Runtime\Narrating;
use Workshop\Kafka\Runtime\OffsetReset;
use Workshop\Kafka\Runtime\RebalanceProtocol;

/**
 * Drives partition assignment on every rebalance, using the assign API that matches
 * the negotiated RebalanceProtocol:
 *
 *  - Cooperative (cooperative-sticky): only the partitions that actually move are
 *    revoked or assigned, via incrementalAssign() / incrementalUnassign(), so a
 *    rebalance no longer stops every consumer in the group.
 *  - Eager (range/roundrobin, or no strategy set): the whole assignment is revoked
 *    and re-handed each rebalance, via assign($all) / assign(null).
 *
 * The protocol is NOT a free choice — librdkafka rejects the wrong API for the
 * negotiated strategy ("must be made using assign() when rebalance protocol type is
 * EAGER"), killing the consumer on its first assignment. So it is derived from the
 * profile's partition.assignment.strategy, never declared independently.
 *
 * It also enforces the run's OffsetReset: for Beginning/End the start offset is
 * stamped onto each partition as it is assigned, so the run reads from the log's
 * edge regardless of any committed offset. Committed (the default) stamps nothing
 * and lets the broker resume from the stored offset.
 */
final readonly class RebalanceCallback implements ConfCallback
{
    use Narrating;

    public function __construct(
        private ?\Closure $narrate = null,
        private OffsetReset $offsetReset = OffsetReset::Committed,
        private RebalanceProtocol $protocol = RebalanceProtocol::Eager,
    ) {
    }

    public function attachTo(Conf $conf): void
    {
        $conf->setRebalanceCb(
            /** @param array<int, TopicPartition>|null $partitions */
            function (KafkaConsumer $consumer, int $err, ?array $partitions = null): void {
                switch ($err) {
                    case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
                        /** @var array<int, TopicPartition> $assigning */
                        $assigning = $partitions ?? [];
                        $assigned = $this->withSeek($assigning);
                        $this->narrate('⇄ assign ' . $this->names($assigned) . $this->seekSuffix());
                        match ($this->protocol) {
                            RebalanceProtocol::Cooperative => $consumer->incrementalAssign($assigned),
                            RebalanceProtocol::Eager => $consumer->assign($assigned),
                        };

                        break;

                    case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
                        $this->narrate('⇄ revoke ' . $this->names($partitions));
                        match ($this->protocol) {
                            RebalanceProtocol::Cooperative => $consumer->incrementalUnassign($partitions ?? []),
                            RebalanceProtocol::Eager => $consumer->assign(null),
                        };

                        break;

                    default:
                        $this->narrate('rebalance error: ' . rd_kafka_err2str($err));
                        $consumer->assign(null);
                }
            });
    }

    /**
     * Stamp the requested start offset onto each newly assigned partition. For
     * Beginning/End librdkafka starts fetching from that offset instead of the
     * committed one; for Committed the list is returned untouched.
     *
     * @param array<int, TopicPartition> $partitions
     *
     * @return array<int, TopicPartition>
     */
    private function withSeek(array $partitions): array
    {
        $offset = $this->offsetReset->seekOffset();
        if (null === $offset) {
            return $partitions;
        }

        foreach ($partitions as $partition) {
            $partition->setOffset($offset);
        }

        return $partitions;
    }

    private function seekSuffix(): string
    {
        return OffsetReset::Committed === $this->offsetReset ? '' : ' @ ' . $this->offsetReset->value;
    }

    /**
     * @param array<int, TopicPartition>|null $partitions
     */
    private function names(?array $partitions): string
    {
        $names = array_map(
            static fn (TopicPartition $p): string => sprintf('%s[%d]', $p->getTopic(), $p->getPartition()),
            $partitions ?? [],
        );

        return implode(', ', $names) ?: '(none)';
    }
}
