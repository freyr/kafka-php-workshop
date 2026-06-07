<?php

declare(strict_types=1);

namespace Workshop\Kafka\Config;

/**
 * A named, immutable bundle of librdkafka settings for one client role — the unit
 * a command selects to get "more than one client with different config sets". The
 * name is the thing you point at on a slide ("Block 3 uses producer.idempotent");
 * the settings are KafkaSetting value objects, so every value still carries its
 * defensible rationale. ConfBuilder turns a profile into a live \RdKafka\Conf.
 */
final readonly class KafkaProfile
{
    /**
     * @param list<KafkaSetting> $settings
     */
    public function __construct(
        public string $name,
        public ClientRole $role,
        public array $settings,
    ) {
    }
}
