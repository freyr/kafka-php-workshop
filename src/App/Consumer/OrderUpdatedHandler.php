<?php

declare(strict_types=1);

namespace Workshop\App\Consumer;

use Doctrine\DBAL\Connection;

/**
 * Records an order.updated status transition into the `orders` projection. Routed
 * by the MessageBus from an OrderUpdatedDto via this handler's __invoke signature.
 */
#[AsMessageHandler]
final readonly class OrderUpdatedHandler
{
    public function __construct(
        private Connection $connection,
    ) {
    }

    public function __invoke(OrderUpdatedDto $dto): void
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
}
