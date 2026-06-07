<?php

declare(strict_types=1);

namespace Workshop\Kafka\Client;

use Workshop\Kafka\Callback\CallbackKit;
use Workshop\Kafka\Callback\ErrorCallback;
use Workshop\Kafka\Config\ConfBuilder;
use Workshop\Kafka\Config\KafkaProfile;
use Workshop\Kafka\Config\ProfileRegistry;
use Workshop\Kafka\Serde\MessageSerializer;

/**
 * Builds a MessageProducer from a named profile. One of three role factories — a
 * producer needs idempotence and a delivery-report callback, never group.id or a
 * rebalance handler, so the producer's config surface is its own class.
 */
final readonly class ProducerFactory
{
    public function __construct(
        private ConfBuilder $confBuilder,
        private ProfileRegistry $profiles,
    ) {
    }

    public function create(string|KafkaProfile $profile, MessageSerializer $serializer, ?CallbackKit $callbacks = null): MessageProducer
    {
        $profile = $profile instanceof KafkaProfile ? $profile : $this->profiles->get($profile);

        $conf = $this->confBuilder->build($profile);
        ($callbacks ?? new CallbackKit(new ErrorCallback()))->attachTo($conf);

        return new MessageProducer(new \RdKafka\Producer($conf), $serializer);
    }
}
