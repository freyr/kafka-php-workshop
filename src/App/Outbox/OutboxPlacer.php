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
     * The EventRouter's routing value: Debezium and outbox:relay append it to the
     * topic prefix (enet.ecommerce.outbox.<Aggregate>). Order events share the
     * Order aggregate; the Block 7 error.demo event routes to its own dedicated
     * topic family so injected failures can never pollute the order topics.
     */
    private const string ORDER_AGGREGATE = 'Order';
    private const array AGGREGATE_TYPES = [
        'error.demo' => 'ErrorDemo',
    ];

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
     * @param bool $poison            corrupt the stored payload bytes — the Block 7
     *                                poison-injection beat: headers stay correct, so
     *                                the message routes to the consumer and then
     *                                fails its decode gate
     *
     * @throws SimulatedCrash          when $crashBeforeCommit is set (after rollback)
     * @throws SchemaRegistryException when the Avro format must encode against an unregistered subject
     * @throws DriverException         when the encoding does not match the provisioned payload column (e.g. AVRO bytes into a JSON column)
     */
    public function place(Message $message, bool $crashBeforeCommit = false, PayloadFormat $format = PayloadFormat::Json, bool $poison = false): void
    {
        // Encoding happens OUTSIDE the transaction on purpose: the AVRO path may
        // call the Schema Registry (cache miss), and a remote lookup has no
        // business holding a database transaction open — same discipline as
        // keeping the broker off the business write's critical path.
        $payload = $this->encode($message, $format);
        if ($poison) {
            $payload = $this->corrupt($payload, $format);
        }

        $this->connection->transactional(function () use ($message, $payload, $crashBeforeCommit): void {
            $eventType = $this->names->nameOf($message);

            // Write 1: the business state change (the "real work"). The error.demo
            // event has no business state — it borrows the outbox only as its
            // at-least-once producing vehicle, so its placement is the outbox
            // append alone.
            $aggregateType = self::AGGREGATE_TYPES[$eventType] ?? self::ORDER_AGGREGATE;
            if (self::ORDER_AGGREGATE === $aggregateType) {
                $this->orders->apply($eventType, $message->partitionKey(), $message->payload);
            }

            // Write 2: the event, appended to the outbox in the SAME transaction.
            $this->outbox->add(
                $message->eventId(),
                $aggregateType,
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
     * Turn a valid encoding into poison the relay will faithfully ship. For AVRO
     * the magic byte survives but the frame's schema id becomes 0xFFFFFFFF — an id
     * the registry will never hold. The consumer routes the message, tries to
     * resolve the writer schema, and the registry says 404: a deterministic decode
     * failure, and the classic real-world poison (someone broke the schema
     * contract). Corrupting the BODY is deliberately not how this works — avro-php
     * happily decodes garbage or even zero bytes into junk default values, so a
     * body corruption is not reliably poison at all. For JSON the whole payload
     * becomes a non-JSON marker. The relay's contract is bytes; it cannot protect
     * the consumer — the consumer defends itself.
     */
    private function corrupt(string $payload, PayloadFormat $format): string
    {
        return match ($format) {
            PayloadFormat::Avro => "\x00\xff\xff\xff\xff" . substr($payload, 5),
            PayloadFormat::Json => 'POISON — not valid JSON, not valid AVRO',
        };
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
