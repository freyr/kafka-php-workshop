<?php

declare(strict_types=1);

namespace Workshop\App\Outbox;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Exception\DriverException;
use FlixTech\SchemaRegistryApi\Exception\SchemaRegistryException;
use Workshop\App\Producer\Message;
use Workshop\App\Producer\MessageNameResolver;
use Workshop\Kafka\Serde\MessageSerializer;

/**
 * The transactional outbox pattern itself, in one method: the order's state
 * change and the event announcing it are written in ONE database transaction.
 * Either both commit — the system changed and the event will reach Kafka (a
 * relay guarantees delivery later) — or both roll back and it is as if nothing
 * happened. No Kafka client anywhere near this class: the business write never
 * waits on a broker, and a broker outage can never half-commit an order.
 *
 * Contrast with Block 5's produce-then-commit, where a crash between the DB
 * commit and the produce loses the event (or the reverse order invents one).
 */
final readonly class OutboxPlacer
{
    /**
     * The EventRouter's routing value: Debezium appends it to the topic prefix
     * (enet.ecommerce.outbox.Order), and outbox:relay mirrors that with its
     * --topic-prefix. Every catalog message is an order event, so it is fixed.
     */
    private const string AGGREGATE_TYPE = 'Order';

    public function __construct(
        private Connection $connection,
        private OutboxRepository $outbox,
        private OrderStateWriter $orders,
        private MessageNameResolver $names,
        private MessageSerializer $serializer,
    ) {
    }

    /**
     * @param bool $crashBeforeCommit throw after both writes, right before
     *                                COMMIT — the rollback demo (--fail)
     *
     * @throws SimulatedCrash          when $crashBeforeCommit is set (after rollback)
     * @throws SchemaRegistryException when the Avro format must encode against an unregistered subject
     * @throws DriverException         when the encoding does not match the provisioned payload column (e.g. AVRO bytes into a JSON column)
     */
    public function place(Message $message, bool $crashBeforeCommit = false, PayloadFormat $format = PayloadFormat::Json): void
    {
        // Encoding happens OUTSIDE the transaction on purpose: the AVRO path may
        // call the Schema Registry (cache miss), and a remote lookup has no
        // business holding a database transaction open — same discipline as
        // keeping the broker off the business write's critical path.
        $payload = $this->encode($message, $format);

        $this->connection->transactional(function () use ($message, $payload, $crashBeforeCommit): void {
            $eventType = $this->names->nameOf($message);

            // Write 1: the business state change (the "real work").
            $this->orders->apply($eventType, $message->partitionKey(), $message->payload);

            // Write 2: the event, appended to the outbox in the SAME transaction.
            $this->outbox->add(
                $message->eventId(),
                self::AGGREGATE_TYPE,
                $message->partitionKey(),
                $eventType,
                $payload,
            );

            if ($crashBeforeCommit) {
                throw new SimulatedCrash('simulated crash after both writes, before COMMIT');
            }
        });
    }

    /**
     * Json stores the same envelope the AVRO path puts on the wire — just
     * JSON-encoded. Avro stores the actual wire bytes: Confluent framing against
     * the message's registered subject, produced by the same MessageSerializer as
     * kafka:produce:sample — so a relayed record is byte-identical to a directly
     * produced one, and kafka:consume can decode it.
     */
    private function encode(Message $message, PayloadFormat $format): string
    {
        return match ($format) {
            PayloadFormat::Json => json_encode($message->envelope(), JSON_THROW_ON_ERROR),
            PayloadFormat::Avro => $this->serializer->encode($message),
        };
    }
}
