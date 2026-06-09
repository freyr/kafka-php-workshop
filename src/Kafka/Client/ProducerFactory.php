<?php

declare(strict_types=1);

namespace Workshop\Kafka\Client;

use Workshop\App\Producer\MessageNameResolver;
use Workshop\App\Producer\MessageRouting;
use Workshop\Kafka\Callback\CallbackKit;
use Workshop\Kafka\Callback\DeliveryTally;
use Workshop\Kafka\Callback\ErrorCallback;
use Workshop\Kafka\Config\ConfBuilder;
use Workshop\Kafka\Config\KafkaProfile;
use Workshop\Kafka\Config\KafkaProfiles;
use Workshop\Kafka\Serde\MessageSerializer;

/**
 * Builds a MessageProducer from a named profile. One of three role factories — a
 * producer needs idempotence and a delivery-report callback, never group.id or a
 * rebalance handler, so the producer's config surface is its own class. The
 * routing table and name resolver are handed to every producer so its produce()
 * can route a Message to its own topic.
 */
final readonly class ProducerFactory
{
    public function __construct(
        private ConfBuilder $confBuilder,
        private KafkaProfiles $profiles,
        private MessageRouting $routing,
        private MessageNameResolver $names,
    ) {
    }

    public function create(string|KafkaProfile $profile, MessageSerializer $serializer, ?CallbackKit $callbacks = null): MessageProducer
    {
        $profile = $profile instanceof KafkaProfile ? $profile : $this->profiles->get($profile);

        $conf = $this->confBuilder->build($profile);
        ($callbacks ?? new CallbackKit(new ErrorCallback()))->attachTo($conf);

        return new MessageProducer(new \RdKafka\Producer($conf), $serializer, $this->routing, $this->names);
    }

    /**
     * The relay flavor: same profile-driven config surface, but the producer
     * carries pre-serialized bytes to explicit topics (no routing table, no
     * serializer) and gets a counting delivery tally instead of a narrating one,
     * so the outbox relay can verify a batch was acked before marking it
     * published.
     */
    public function createRaw(string|KafkaProfile $profile, ?\Closure $narrate = null): RawProducer
    {
        $profile = $profile instanceof KafkaProfile ? $profile : $this->profiles->get($profile);

        $conf = $this->confBuilder->build($profile);
        $tally = new DeliveryTally($narrate);
        new CallbackKit($tally, new ErrorCallback($narrate))->attachTo($conf);

        return new RawProducer(new \RdKafka\Producer($conf), $tally);
    }
}
