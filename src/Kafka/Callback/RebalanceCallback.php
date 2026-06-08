<?php

declare(strict_types=1);

namespace Workshop\Kafka\Callback;

use RdKafka\Conf;
use RdKafka\KafkaConsumer;
use RdKafka\TopicPartition;

/**
 * Cooperative-sticky rebalancing. Instead of the eager assign()/assign(null)
 * dance, only the partitions that actually move are revoked or assigned
 * incrementally, so a rebalance no longer stops every consumer in the group.
 * Lifted from the Block 8 kafka:config:stats command so every consumer profile gets it.
 */
final readonly class RebalanceCallback implements ConfCallback
{
    use Narrating;

    public function __construct(
        private ?\Closure $narrate = null,
    ) {
    }

    public function attachTo(Conf $conf): void
    {
        $conf->setRebalanceCb(
            /** @param array<int, TopicPartition>|null $partitions */
            function (KafkaConsumer $consumer, int $err, ?array $partitions = null): void {
                switch ($err) {
                    case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
                        $this->narrate('⇄ assign ' . $this->names($partitions));
                        $consumer->incrementalAssign($partitions ?? []);

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
