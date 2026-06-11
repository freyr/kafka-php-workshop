<?php

declare(strict_types=1);

namespace Workshop\Enqueue;

use Enqueue\RdKafka\RdKafkaContext;
use Workshop\Kafka\Callback\DeliveryTally;

/**
 * Builds an enqueue RdKafkaContext per client role, each with its own
 * production-grade librdkafka settings. The enqueue layer's counterpart to
 * KafkaProfiles + ConfBuilder: where the pure-rdkafka path composes a profile and
 * builds a Conf, enqueue takes the same keys as a plain config array and wires
 * them onto its context internally.
 *
 * Three roles, three different reliability postures:
 *
 *  - fpmProducer: the occasional php-fpm producer — one message per web request.
 *    Durable (acks=all) but NOT idempotent: at one message per request there is no
 *    retry-reorder window worth the idempotence handshake; max.in.flight=1 closes
 *    it anyway. The request wants its delivery report before responding, so the
 *    timeout is seconds, not librdkafka's 5-minute default.
 *
 *  - relayProducer: the long-running outbox relay — a batch publisher that CAN
 *    afford the full reliability stack, so it gets idempotence, compression and a
 *    linger window, plus a counting delivery-report callback so the relay marks
 *    rows published only on the broker's word.
 *
 *  - consumer: explicit commit (enqueue's acknowledge() = commit AFTER the handler
 *    returns — at-least-once), synchronous so a commit failure surfaces in the
 *    loop, reading from earliest when the group has no committed offset.
 *
 * The settings methods are public and pure so the values stay testable and
 * defensible without touching a broker.
 */
final readonly class EnqueueContextFactory
{
    public function __construct(
        private string $brokers,
    ) {
    }

    public function fpmProducer(): RdKafkaContext
    {
        return $this->context([
            'global' => $this->fpmProducerSettings(),
            // Last-resort backstop: if the request forgets to flush, the shutdown
            // hook enqueue registers drains the queue for at most this long instead
            // of blocking the fpm worker forever (-1, the enqueue default).
            'shutdown_timeout' => 10000,
        ]);
    }

    public function relayProducer(DeliveryTally $deliveries): RdKafkaContext
    {
        return $this->context([
            'global' => $this->relayProducerSettings(),
            'shutdown_timeout' => 10000,
            // The same counting delivery-report callback the pure-rdkafka relay
            // uses — enqueue passes it straight to Conf::setDrMsgCb.
            'dr_msg_cb' => static function (\RdKafka\Producer $producer, \RdKafka\Message $message) use ($deliveries): void {
                $deliveries->record($message);
            },
        ]);
    }

    public function consumer(string $group): RdKafkaContext
    {
        return $this->context([
            'global' => $this->consumerSettings($group),
            // acknowledge() commits synchronously: the loop learns about a failed
            // commit right away instead of discovering it at the next rebalance.
            'commit_async' => false,
        ]);
    }

    /**
     * The php-fpm request producer. One message per request, delivery report
     * before the response leaves — durability without the idempotence handshake.
     *
     * @return array<string, string>
     */
    public function fpmProducerSettings(): array
    {
        return [
            'metadata.broker.list' => $this->brokers,
            'client.id' => sprintf('workshop.enqueue-fpm.%d', getmypid()),
            // All in-sync replicas ack before the delivery report — the produced
            // message survives a broker failover.
            'acks' => 'all',
            // One request in flight: a retry can never overtake a newer message, so
            // ordering holds WITHOUT enable.idempotence — at one message per web
            // request the PID handshake and its broker round-trips buy nothing.
            'max.in.flight.requests.per.connection' => '1',
            // No batching window: the request is about to flush anyway, and an fpm
            // worker has no later send to batch with.
            'linger.ms' => '0',
            // Fail the request in seconds, not librdkafka's 5-minute default — a
            // web request cannot hold its response while the producer retries; it
            // crashes loudly instead and the user/caller retries the request.
            'message.timeout.ms' => '10000',
        ];
    }

    /**
     * The long-running relay producer: the full reliability stack, same rationale
     * as the pure-rdkafka producer.idempotent profile.
     *
     * @return array<string, string>
     */
    public function relayProducerSettings(): array
    {
        return [
            'metadata.broker.list' => $this->brokers,
            'client.id' => sprintf('workshop.enqueue-relay.%d', getmypid()),
            // Exactly-once delivery to the broker; auto-sets acks=all,
            // max.in.flight=5, retries=MAX_INT. A daemon pays the handshake once.
            'enable.idempotence' => 'true',
            'acks' => 'all',
            'compression.type' => 'lz4',
            // Wait up to 50ms to fill a batch — a relay drains a backlog, so a
            // little latency buys real batching + compression.
            'linger.ms' => '50',
        ];
    }

    /**
     * The committing consumer: offsets advance only when the application says so.
     *
     * @return array<string, string>
     */
    public function consumerSettings(string $group): array
    {
        return [
            'metadata.broker.list' => $this->brokers,
            'client.id' => sprintf('workshop.enqueue-consume.%d', getmypid()),
            'group.id' => $group,
            // The key one: enqueue's acknowledge() commits AFTER the handler
            // returns = at-least-once; the bus's dedup middleware turns redelivery
            // into a no-op (effectively-once). Auto-commit could advance the offset
            // before the handler finishes and lose the message on a crash.
            'enable.auto.commit' => 'false',
            // With no committed offset, start from the beginning so a new group
            // processes the full backlog, not just new events.
            'auto.offset.reset' => 'earliest',
        ];
    }

    /**
     * @param array{global: array<string, string>, shutdown_timeout?: int, commit_async?: bool, dr_msg_cb?: callable} $config
     */
    private function context(array $config): RdKafkaContext
    {
        $context = new RdKafkaContext($config);
        // Raw bytes on the wire (see RawBodySerializer) — never enqueue's JSON envelope.
        $context->setSerializer(new RawBodySerializer());

        return $context;
    }
}
