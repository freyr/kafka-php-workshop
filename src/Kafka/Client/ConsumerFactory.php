<?php

declare(strict_types=1);

namespace Workshop\Kafka\Client;

use RdKafka\KafkaConsumer;
use Workshop\Kafka\Callback\CallbackKit;
use Workshop\Kafka\Callback\ErrorCallback;
use Workshop\Kafka\Callback\RebalanceCallback;
use Workshop\Kafka\Config\ConfBuilder;
use Workshop\Kafka\Config\KafkaProfile;
use Workshop\Kafka\Config\KafkaProfiles;
use Workshop\Kafka\Runtime\OffsetReset;
use Workshop\Kafka\Runtime\RebalanceProtocol;

/**
 * Builds a MessageConsumer from a named profile and a group id — the consume-side
 * mirror of ProducerFactory. Construction (this factory) and the consume loop
 * (MessageConsumer::run) are deliberately two steps, so each is teachable on its
 * own. The wrapped \RdKafka\KafkaConsumer is not subscribed yet — run() subscribes
 * when it starts.
 *
 * The factory owns the consumer's CallbackKit so the rebalance callback always uses
 * the assign API that matches the profile's negotiated rebalance protocol — the one
 * place that has the resolved profile, so the two can never drift.
 */
final readonly class ConsumerFactory
{
    public function __construct(
        private ConfBuilder $confBuilder,
        private KafkaProfiles $profiles,
    ) {
    }

    /**
     * @param array<string, string|int> $overrides extra librdkafka settings applied
     *                                             last (e.g. a commit strategy's
     *                                             enable.auto.commit); group.id is
     *                                             always set and wins over them
     */
    public function create(
        string|KafkaProfile $profile,
        string $groupId,
        OffsetReset $offsetReset = OffsetReset::Committed,
        ?\Closure $narrate = null,
        array $overrides = [],
    ): MessageConsumer {
        $profile = $profile instanceof KafkaProfile ? $profile : $this->profiles->get($profile);

        $conf = $this->confBuilder->build($profile, [
            ...$overrides,
            'group.id' => $groupId,
        ]);

        // The rebalance protocol is dictated by the profile's assignment strategy:
        // calling the cooperative assign API under an eager assignor (or vice versa)
        // makes librdkafka throw on the first assignment.
        $protocol = RebalanceProtocol::fromAssignmentStrategy(
            $profile->setting('partition.assignment.strategy'),
        );
        $callbacks = new CallbackKit(
            new RebalanceCallback($narrate, $offsetReset, $protocol),
            new ErrorCallback($narrate),
        );
        $callbacks->attachTo($conf);

        return new MessageConsumer(new KafkaConsumer($conf));
    }
}
