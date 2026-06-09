<?php

declare(strict_types=1);

namespace Workshop\Tests\Integration\Support;

/**
 * Reads a topic's per-partition end offsets straight from the broker (low-level
 * metadata + watermark queries, no consumer group). Integration tests snapshot the
 * watermarks before producing and assert the delta afterwards, so assertions stay
 * correct on shared topics whose logs accumulate across the suite run.
 */
final class OffsetProbe
{
    private const int TIMEOUT_MS = 10000;

    private readonly \RdKafka\Consumer $client;

    public function __construct(string $brokers)
    {
        $conf = new \RdKafka\Conf();
        $conf->set('metadata.broker.list', $brokers);
        $this->client = new \RdKafka\Consumer($conf);
    }

    /**
     * @return array<int, int> partition id => end (high watermark) offset
     */
    public function endOffsets(string $topic): array
    {
        $metadata = $this->client->getMetadata(false, $this->client->newTopic($topic), self::TIMEOUT_MS);

        $ends = [];
        foreach ($metadata->getTopics() as $topicMetadata) {
            if (! $topicMetadata instanceof \RdKafka\Metadata\Topic || $topicMetadata->getTopic() !== $topic) {
                continue;
            }
            foreach ($topicMetadata->getPartitions() as $partitionMetadata) {
                if (! $partitionMetadata instanceof \RdKafka\Metadata\Partition) {
                    continue;
                }
                $low = 0;
                $high = 0;
                $this->client->queryWatermarkOffsets($topic, $partitionMetadata->getId(), $low, $high, self::TIMEOUT_MS);
                $ends[$partitionMetadata->getId()] = $high;
            }
        }
        ksort($ends);

        return $ends;
    }

    /**
     * The topic's total message count since creation — the deterministic --max for
     * a from-beginning consume of the whole backlog.
     */
    public function totalEnd(string $topic): int
    {
        return array_sum($this->endOffsets($topic));
    }
}
