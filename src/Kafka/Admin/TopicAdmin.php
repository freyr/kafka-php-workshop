<?php

declare(strict_types=1);

namespace Workshop\Kafka\Admin;

use RdKafka\Metadata;
use RdKafka\Producer;

/**
 * Read-only topic inspection over the raw \RdKafka metadata API.
 *
 * php-rdkafka (librdkafka 6.x) exposes cluster *metadata* — it can list topics and
 * count their partitions — but it has NO create/delete/alter admin API
 * (RdKafka\NewTopic does not exist; there is no RdKafka\Admin namespace). That
 * limitation is the Block 2 teaching point: provisioning still belongs to the
 * kafka CLI, so topic creation/deletion stays in the bin/ shell scripts
 * (bin/topic-create, bin/topic-map, bin/topic-delete). This class is the PHP half
 * of the "both" approach — inspection in PHP, mutation in the CLI.
 */
final readonly class TopicAdmin
{
    public function __construct(
        private Producer $client,
        private int $timeoutMs = 10000,
    ) {
    }

    /**
     * @return list<string> all topic names, sorted
     */
    public function list(): array
    {
        $names = [];
        foreach ($this->metadata()->getTopics() as $topic) {
            $names[] = $topic->getTopic();
        }
        sort($names);

        return $names;
    }

    public function exists(string $name): bool
    {
        foreach ($this->metadata()->getTopics() as $topic) {
            if ($topic->getTopic() === $name) {
                return true;
            }
        }

        return false;
    }

    public function partitionCount(string $name): int
    {
        return $this->describe($name)['partitions'];
    }

    /**
     * @return array{name: string, partitions: int, partition_ids: list<int>}
     */
    public function describe(string $name): array
    {
        foreach ($this->metadata()->getTopics() as $topic) {
            if ($topic->getTopic() !== $name) {
                continue;
            }

            $ids = [];
            foreach ($topic->getPartitions() as $partition) {
                $ids[] = $partition->getId();
            }
            sort($ids);

            return [
                'name' => $name,
                'partitions' => count($ids),
                'partition_ids' => $ids,
            ];
        }

        throw new \RuntimeException(sprintf('Topic "%s" does not exist.', $name));
    }

    private function metadata(): Metadata
    {
        // all_topics = true so a missing topic is simply absent from the list
        // rather than triggering auto-create or an error.
        return $this->client->getMetadata(true, null, $this->timeoutMs);
    }
}
