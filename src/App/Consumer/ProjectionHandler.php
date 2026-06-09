<?php

declare(strict_types=1);

namespace Workshop\App\Consumer;

use Doctrine\DBAL\Connection;

/**
 * The one generic handler the ConsumerBus dispatches to. It takes only the DTO —
 * never the envelope — so idempotency and transactions stay outside it as bus
 * middleware (the simplified command-handler / command-bus shape). Its single job
 * is to fold each order event into the `orders` read-model projection:
 *
 *   created   → upsert the order row (open)
 *   updated   → record the new status and where it came from
 *   cancelled → mark cancelled with a reason
 *
 * A DTO type it does not recognise is ignored — the routing table only ever hands
 * it types it declared, so this is defensive, not a branch the room exercises.
 */
final readonly class ProjectionHandler implements DtoHandler
{
    public function __construct(
        private Connection $connection,
    ) {
    }

    public function handle(object $dto): void
    {
        match (true) {
            $dto instanceof OrderCreatedDto => $this->onCreated($dto),
            $dto instanceof OrderUpdatedDto => $this->onUpdated($dto),
            $dto instanceof OrderCancelledDto => $this->onCancelled($dto),
            default => null,
        };
    }

    private function onCreated(OrderCreatedDto $dto): void
    {
        // A redelivered create (at-least-once) must not clobber a status the order
        // has since moved to, so the upsert only refreshes the descriptive columns.
        $this->connection->executeStatement(
            <<<'SQL'
                INSERT INTO orders (order_id, customer_name, total_cents, status)
                VALUES (:order_id, :customer_name, :total_cents, 'open') AS incoming
                ON DUPLICATE KEY UPDATE
                    customer_name = incoming.customer_name,
                    total_cents   = incoming.total_cents
                SQL,
            [
                'order_id' => $dto->orderId,
                'customer_name' => $dto->customer->displayName,
                'total_cents' => $dto->totals->total->amountCents,
            ],
        );
    }

    private function onUpdated(OrderUpdatedDto $dto): void
    {
        $this->connection->executeStatement(
            <<<'SQL'
                INSERT INTO orders (order_id, status, previous_status)
                VALUES (:order_id, :status, :previous_status) AS incoming
                ON DUPLICATE KEY UPDATE
                    status          = incoming.status,
                    previous_status = incoming.previous_status
                SQL,
            [
                'order_id' => $dto->orderId,
                'status' => $dto->status,
                'previous_status' => $dto->previousStatus,
            ],
        );
    }

    private function onCancelled(OrderCancelledDto $dto): void
    {
        $this->connection->executeStatement(
            <<<'SQL'
                INSERT INTO orders (order_id, status, cancelled_reason)
                VALUES (:order_id, 'cancelled', :cancelled_reason) AS incoming
                ON DUPLICATE KEY UPDATE
                    status           = 'cancelled',
                    cancelled_reason = incoming.cancelled_reason
                SQL,
            [
                'order_id' => $dto->orderId,
                'cancelled_reason' => $dto->reason,
            ],
        );
    }
}
