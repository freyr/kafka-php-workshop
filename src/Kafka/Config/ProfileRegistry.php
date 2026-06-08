<?php

declare(strict_types=1);

namespace Workshop\Kafka\Config;

/**
 * The catalog of named client profiles the workshop ships. Each profile is built
 * by selecting groups out of KafkaTuning's single source of truth, so the values
 * a profile applies are exactly the values kafka:config:show can defend.
 *
 * This is the multi-client seam: a command picks a profile by name and the
 * factory builds the matching client. Later blocks extend the workshop by adding
 * entries here (B5 producer.transactional / consumer.read-committed, B7
 * consumer.retry / producer.dlq, B8 consumer.observer-stats) — no factory changes.
 *
 * Following the repo convention (KafkaTuning), the registry owns its collection
 * internally rather than relying on tagged-service iteration.
 */
final readonly class ProfileRegistry
{
    public function __construct(
        private KafkaTuning $tuning,
    ) {
    }

    public function get(string $name): KafkaProfile
    {
        foreach ($this->all() as $profile) {
            if ($profile->name === $name) {
                return $profile;
            }
        }

        $known = implode(', ', array_map(static fn (KafkaProfile $p): string => $p->name, $this->all()));

        throw new \InvalidArgumentException(sprintf('Unknown Kafka profile "%s". Known profiles: %s', $name, $known));
    }

    /**
     * @return list<KafkaProfile>
     */
    public function all(): array
    {
        return [
            // Block 1: deliberately bare — librdkafka's own defaults, so the room
            // can see the cost of an untuned producer before we add reliability.
            new KafkaProfile('producer.simple', ClientRole::Producer, []),

            // Block 3: exactly-once-to-broker AVRO producer. idempotence + acks +
            // lz4 compression, the trio that moves together.
            new KafkaProfile('producer.idempotent', ClientRole::Producer, $this->pick($this->tuning->producer(), ['reliability', 'compression'])),

            // Block 1/3: named group, explicit commit, static membership, and
            // cooperative-sticky rebalancing — the production consumer baseline.
            // group.instance.id gives each member a stable identity, so a restart
            // rejoins its partitions WITHOUT a rebalance.
            new KafkaProfile('consumer.at-least-once', ClientRole::Consumer, $this->pick($this->tuning->consumer(), ['offset', 'assignment'])),

            // Same named, committing consumer WITHOUT group.instance.id: dynamic
            // membership, so every join/leave triggers a (cooperative) rebalance.
            // The deliberate contrast to consumer.at-least-once — kafka:consume picks
            // between the two to show how static membership changes rebalancing.
            new KafkaProfile('consumer.dynamic', ClientRole::Consumer, $this->pick($this->tuning->consumer(), ['offset', 'assignment'], ['group.instance.id'])),

            // Block 1: throwaway group that reads the whole log from earliest.
            // No group.instance.id — static membership would fence a re-run.
            new KafkaProfile('consumer.ephemeral', ClientRole::Consumer, $this->pick($this->tuning->consumer(), ['offset', 'assignment'], ['group.instance.id'])),
        ];
    }

    /**
     * @param list<KafkaSetting> $settings
     * @param list<string>       $groups
     * @param list<string>       $excludeKeys
     *
     * @return list<KafkaSetting>
     */
    private function pick(array $settings, array $groups, array $excludeKeys = []): array
    {
        return array_values(array_filter(
            $settings,
            static fn (KafkaSetting $s): bool => in_array($s->group, $groups, true) && ! in_array($s->key, $excludeKeys, true),
        ));
    }
}
