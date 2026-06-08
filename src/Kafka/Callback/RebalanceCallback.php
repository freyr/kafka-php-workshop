<?php

declare(strict_types=1);

namespace Workshop\Kafka\Callback;

use RdKafka\Conf;
use RdKafka\KafkaConsumer;
use RdKafka\TopicPartition;
use Workshop\Kafka\Runtime\OffsetReset;

/**
 * Cooperative-sticky rebalancing. Instead of the eager assign()/assign(null)
 * dance, only the partitions that actually move are revoked or assigned
 * incrementally, so a rebalance no longer stops every consumer in the group.
 * Lifted from the Block 8 kafka:config:stats command so every consumer profile gets it.
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
                        $consumer->incrementalAssign($assigned);

                        break;

                    case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
                        $this->narrate('⇄ revoke ' . $this->names($partitions));
                        $consumer->incrementalUnassign($partitions ?? []);

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
