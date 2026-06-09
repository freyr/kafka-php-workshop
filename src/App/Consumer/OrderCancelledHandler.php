<?php

declare(strict_types=1);

namespace Workshop\App\Consumer;

use Doctrine\DBAL\Connection;

/**
 * Marks an order cancelled (with its reason) in the `orders` projection. Routed by
 * the MessageBus from an OrderCancelledDto via this handler's __invoke signature.
 */
#[AsMessageHandler]
final readonly class OrderCancelledHandler
{
    public function __construct(
        private Connection $connection,
    ) {
    }

    public function __invoke(OrderCancelledDto $dto): void
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
