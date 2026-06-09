<?php

declare(strict_types=1);

namespace Workshop\App\Consumer;

use Doctrine\DBAL\Connection;

/**
 * Folds an order.created event into the `orders` read-model projection. It takes
 * only the DTO — never the envelope — so idempotency and transactions stay outside
 * it as bus middleware (the command-handler / command-bus shape). The MessageBus
 * routes an OrderCreatedDto here by matching this handler's __invoke signature.
 */
#[AsMessageHandler]
final readonly class OrderCreatedHandler
{
    public function __construct(
        private Connection $connection,
    ) {
    }

    public function __invoke(OrderCreatedDto $dto): void
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
}
