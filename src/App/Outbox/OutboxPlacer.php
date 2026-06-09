<?php

declare(strict_types=1);

namespace Workshop\App\Outbox;

use Doctrine\DBAL\Connection;
use Workshop\App\Producer\Message;
use Workshop\App\Producer\MessageNameResolver;

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
    ) {
    }

    /**
     * @param bool $crashBeforeCommit throw after both writes, right before
     *                                COMMIT — the rollback demo (--fail)
     *
     * @throws SimulatedCrash when $crashBeforeCommit is set (after rollback)
     */
    public function place(Message $message, bool $crashBeforeCommit = false): void
    {
        $this->connection->transactional(function () use ($message, $crashBeforeCommit): void {
            $eventType = $this->names->nameOf($message);

            // Write 1: the business state change (the "real work").
            $this->orders->apply($eventType, $message->partitionKey(), $message->payload);

            // Write 2: the event, appended to the outbox in the SAME transaction.
            // The stored payload is the same envelope the AVRO path puts on the
            // wire — metadata.event_id + business fields — just JSON-encoded.
            $this->outbox->add(
                $message->eventId(),
                self::AGGREGATE_TYPE,
                $message->partitionKey(),
                $eventType,
                json_encode($message->envelope(), JSON_THROW_ON_ERROR),
            );

            if ($crashBeforeCommit) {
                throw new SimulatedCrash('simulated crash after both writes, before COMMIT');
            }
        });
    }
}
