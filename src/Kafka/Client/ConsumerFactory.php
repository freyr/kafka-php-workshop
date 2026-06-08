<?php

declare(strict_types=1);

namespace Workshop\Kafka\Client;

use RdKafka\KafkaConsumer;
use Workshop\Kafka\Callback\CallbackKit;
use Workshop\Kafka\Callback\ErrorCallback;
use Workshop\Kafka\Callback\RebalanceCallback;
use Workshop\Kafka\Config\ConfBuilder;
use Workshop\Kafka\Config\KafkaProfile;
use Workshop\Kafka\Config\ProfileRegistry;

/**
 * Builds a configured \RdKafka\KafkaConsumer from a named profile and a group id.
 * Returns the raw consumer rather than a pre-bound runner: construction (this
 * factory) and the consume loop (ConsumerRunner) are deliberately two steps, so
 * each is teachable on its own. The consumer is not subscribed yet — the runner
 * subscribes when it starts.
 */
final readonly class ConsumerFactory
{
    public function __construct(
        private ConfBuilder $confBuilder,
        private ProfileRegistry $profiles,
    ) {
    }

    public function create(string|KafkaProfile $profile, string $groupId, ?CallbackKit $callbacks = null): KafkaConsumer
    {
        $profile = $profile instanceof KafkaProfile ? $profile : $this->profiles->get($profile);

        $conf = $this->confBuilder->build($profile, [
            'group.id' => $groupId,
        ]);
        ($callbacks ?? new CallbackKit(new RebalanceCallback(), new ErrorCallback()))->attachTo($conf);

        return new KafkaConsumer($conf);
    }
}
