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
final class KafkaContextFactory
{
    public function __construct(
        private readonly string $brokers,
        private readonly RawStringSerializer $serializer,
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
            'group.id'           => $groupId,
            'enable.auto.commit' => 'false',
            'auto.offset.reset'  => 'earliest',
        ], $overrides);
    }

    /**
     * @param array<string, string|int> $defaults
     * @param array<string, string|int> $overrides
     */
    private function build(array $defaults, array $overrides): RdKafkaContext
    {
        $global = array_merge(
            ['metadata.broker.list' => $this->brokers],
            $defaults,
            $overrides,
        );

        $factory = new RdKafkaConnectionFactory(['global' => $global]);
        $context = $factory->createContext();
        $context->setSerializer($this->serializer);

        return $context;
    }
}
