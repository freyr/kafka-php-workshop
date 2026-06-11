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
     * @param bool   $crashBeforeCommit throw after both writes, right before
     *                                  COMMIT — the rollback demo (--fail)
     * @param Tamper $tamper            the Block 7 failure-injection kind: break the
     *                                  schema reference (poison), drop the
     *                                  message-name convention header (poison), or
     *                                  corrupt the body under a valid schema id
     *                                  (NOT poison — decodes silently into junk)
     *
     * @throws SimulatedCrash          when $crashBeforeCommit is set (after rollback)
     * @throws SchemaRegistryException when encoding against an unregistered subject
     * @throws DriverException         when the write does not fit the provisioned payload column
     */
    public function place(Message $message, bool $crashBeforeCommit = false, Tamper $tamper = Tamper::None): void
    {
        // The payload is the actual wire bytes: Confluent framing against the
        // message's registered subject, produced by the same MessageSerializer as
        // kafka:produce:sample — so a relayed record is byte-identical to a
        // directly produced one, and kafka:consume can decode it. Encoding happens
        // OUTSIDE the transaction on purpose: the serializer may call the Schema
        // Registry (cache miss), and a remote lookup has no business holding a
        // database transaction open — same discipline as keeping the broker off
        // the business write's critical path.
        $payload = $this->corrupt($this->serializer->encode($message), $tamper);

        $this->connection->transactional(function () use ($message, $payload, $crashBeforeCommit, $tamper): void {
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
            // A Headerless tamper stores an empty event_type: the relay derives the
            // message-name header from that column, so the record ships WITHOUT the
            // routing convention — the payload itself stays perfectly valid.
            $this->outbox->add(
                $message->eventId(),
                $aggregateType,
                $message->partitionKey(),
                Tamper::Headerless === $tamper ? '' : $eventType,
                $payload,
            );

            if ($crashBeforeCommit) {
                throw new SimulatedCrash('simulated crash after both writes, before COMMIT');
            }
        });
    }

    /**
     * Apply the payload half of a tamper (Headerless does not touch the payload —
     * its violation is the missing convention header, applied at the outbox row).
     *
     * Unframed strips the whole 5-byte Confluent frame, leaving the raw AVRO body
     * — what a producer using the wrong serializer would actually ship. The whole
     * frame goes, not just the magic byte: the schema-id bytes begin with 0x00,
     * so dropping the magic byte alone can still look framed. The consumer's
     * frame check then fails locally and deterministically. (Corrupting the body
     * under an intact frame would NOT be poison — avro-php decodes garbage into
     * the schema's defaults without throwing.) The relay's contract is bytes; it
     * cannot protect the consumer — the consumer defends itself.
     */
    private function corrupt(string $payload, Tamper $tamper): string
    {
        return Tamper::Unframed === $tamper ? substr($payload, 5) : $payload;
    }
}
