<?php

declare(strict_types=1);

namespace Workshop\Kafka\Client;

use RdKafka\KafkaConsumer;
use Workshop\Kafka\Callback\CallbackKit;
use Workshop\Kafka\Callback\ErrorCallback;
use Workshop\Kafka\Callback\OffsetCommitCallback;
use Workshop\Kafka\Callback\RebalanceCallback;
use Workshop\Kafka\Config\ConfBuilder;
use Workshop\Kafka\Config\KafkaProfile;
use Workshop\Kafka\Config\KafkaProfiles;
use Workshop\Kafka\Runtime\CommitPolicy;
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
        CommitPolicy $commitPolicy = CommitPolicy::None,
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
        $callbacks = [
            new RebalanceCallback($narrate, $offsetReset, $protocol),
            new ErrorCallback($narrate),
        ];

        // Only the async commit policy needs an offset-commit callback. A fire-and-
        // forget commitAsync() has no caller to receive a rejection, so librdkafka
        // logs a raw COMMITFAIL warning from its own thread for every commit a
        // rebalance rejects; registering the callback both silences that line and
        // narrates the benign rejection as a skip. The synchronous policies already
        // get the result from commit() itself — registering the callback there would
        // only double-report — so it is left off for them.
        if (CommitPolicy::AsyncAfterEachMessage === $commitPolicy) {
            $callbacks[] = new OffsetCommitCallback($narrate);
        }

        (new CallbackKit(...$callbacks))->attachTo($conf);

        return new MessageConsumer(new KafkaConsumer($conf));
    }
}
