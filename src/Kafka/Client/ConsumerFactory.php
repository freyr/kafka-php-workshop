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
 * Builds a MessageConsumer from a named profile and a group id — the consume-side
 * mirror of ProducerFactory. Construction (this factory) and the consume loop
 * (MessageConsumer::run) are deliberately two steps, so each is teachable on its
 * own. The wrapped \RdKafka\KafkaConsumer is not subscribed yet — run() subscribes
 * when it starts.
 */
final readonly class ConsumerFactory
{
    public function __construct(
        private ConfBuilder $confBuilder,
        private ProfileRegistry $profiles,
    ) {
    }

    /**
     * @param array<string, string|int> $overrides extra librdkafka settings applied
     *                                             last (e.g. a commit strategy's
     *                                             enable.auto.commit); group.id is
     *                                             always set and wins over them
     */
    public function create(string|KafkaProfile $profile, string $groupId, ?CallbackKit $callbacks = null, array $overrides = []): MessageConsumer
    {
        $profile = $profile instanceof KafkaProfile ? $profile : $this->profiles->get($profile);

        $conf = $this->confBuilder->build($profile, [
            ...$overrides,
            'group.id' => $groupId,
        ]);
        ($callbacks ?? new CallbackKit(new RebalanceCallback(), new ErrorCallback()))->attachTo($conf);

        return new MessageConsumer(new KafkaConsumer($conf));
    }
}
