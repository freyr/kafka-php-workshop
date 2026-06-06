<?php

declare(strict_types=1);

namespace Workshop\Kernel;

use Enqueue\RdKafka\RdKafkaConnectionFactory;
use Enqueue\RdKafka\RdKafkaContext;

/**
 * Service that builds an RdKafkaContext with the workshop's defaults.
 *
 * Owns only the boilerplate with no pedagogical value: broker discovery from
 * the injected configuration, the raw-string serializer convention, and the
 * sensible defaults shared by every consumer. Anything a demo is actively
 * teaching (group.id, auto.offset.reset overrides, commit mode) stays in the
 * command class itself.
 */
final readonly class KafkaContextFactory
{
    public function __construct(
        private string $brokers,
        private RawStringSerializer $serializer,
    ) {
    }

    /**
     * @param array<string, string|int> $overrides librdkafka 'global' overrides
     */
    public function forProducer(array $overrides = []): RdKafkaContext
    {
        return $this->build([], $overrides);
    }

    /**
     * @param array<string, string|int> $overrides librdkafka 'global' overrides
     */
    public function forConsumer(string $groupId, array $overrides = []): RdKafkaContext
    {
        return $this->build([
            'group.id' => $groupId,
            'enable.auto.commit' => 'false',
            'auto.offset.reset' => 'earliest',
            // Cooperative (incremental) rebalancing for every consumer: only the
            // partitions that move are revoked, the rest keep processing — no
            // stop-the-world rebalance. librdkafka handles the incremental
            // assign/unassign internally on the subscribe() path, so no custom
            // rebalance callback is needed through enqueue. All members of a group
            // must share this strategy, which holds here (each command is its own
            // group). Raw config:stats sets the same value explicitly (Block 8).
            'partition.assignment.strategy' => 'cooperative-sticky',
        ], $overrides);
    }

    /**
     * @param array<string, string|int> $defaults
     * @param array<string, string|int> $overrides
     */
    private function build(array $defaults, array $overrides): RdKafkaContext
    {
        $this->assertBrokerReachable();

        $global = array_merge(
            [
                'metadata.broker.list' => $this->brokers,
            ],
            $defaults,
            $overrides,
        );

        $factory = new RdKafkaConnectionFactory([
            'global' => $global,
        ]);
        $context = $factory->createContext();
        $context->setSerializer($this->serializer);

        return $context;
    }

    /**
     * Quick TCP probe of the first broker so we can surface a friendly hint
     * instead of letting librdkafka time out silently in the background.
     */
    private function assertBrokerReachable(): void
    {
        $first = explode(',', $this->brokers)[0];
        if (! str_contains($first, ':')) {
            return; // unusual format — let librdkafka handle it
        }

        [$host, $port] = explode(':', $first, 2);
        $errno = 0;
        $errstr = '';
        $sock = @fsockopen($host, (int) $port, $errno, $errstr, 2.0);

        if (false === $sock) {
            throw new BrokerUnreachableException($this->brokers, $errstr);
        }

        fclose($sock);
    }
}
