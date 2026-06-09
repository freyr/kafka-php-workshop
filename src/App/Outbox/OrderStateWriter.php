<?php

declare(strict_types=1);

namespace Workshop\App\Outbox;

use Doctrine\DBAL\Connection;

/**
 * The "business logic" half of the outbox simulation: applies an order event's
 * state change to the `orders` table, which Block 6 treats as the system of
 * record (in production the source table and the consumer's read model would be
 * different databases — the workshop reuses one). The SQL mirrors the consume-side
 * handlers on purpose, so an order placed here looks exactly like one projected
 * from the topic. Always runs inside OutboxPlacer's transaction.
 */
final readonly class OrderStateWriter
{
    public function __construct(
        private Connection $connection,
    ) {
    }

    /**
     * @param array<string, mixed> $payload the message's business payload
     */
    public function apply(string $eventType, string $orderId, array $payload): void
    {
        match ($eventType) {
            'order.created' => $this->created($orderId, $payload),
            'order.updated' => $this->updated($orderId, $payload),
            'order.cancelled' => $this->cancelled($orderId, $payload),
            default => throw new \InvalidArgumentException(sprintf("No order state change defined for '%s' — outbox:place only simulates state-changing order events.", $eventType)),
        };
    }

    /**
     * @param array<string, mixed> $payload
     */
    private function created(string $orderId, array $payload): void
    {
        $customer = is_array($payload['customer'] ?? null) ? $payload['customer'] : [];
        $totals = is_array($payload['totals'] ?? null) ? $payload['totals'] : [];
        $total = is_array($totals['total'] ?? null) ? $totals['total'] : [];

        $displayName = $customer['display_name'] ?? null;
        $amountCents = $total['amount_cents'] ?? null;

        $this->connection->executeStatement(
            <<<'SQL'
                INSERT INTO orders (order_id, customer_name, total_cents, status)
                VALUES (:order_id, :customer_name, :total_cents, 'open') AS incoming
                ON DUPLICATE KEY UPDATE
                    customer_name = incoming.customer_name,
                    total_cents   = incoming.total_cents
                SQL,
            [
                'order_id' => $orderId,
                'customer_name' => is_string($displayName) ? $displayName : null,
                'total_cents' => is_int($amountCents) ? $amountCents : null,
            ],
        );
    }

    /**
     * @param array<string, mixed> $payload
     */
    private function updated(string $orderId, array $payload): void
    {
        $status = $payload['status'] ?? null;
        $previous = $payload['previous_status'] ?? null;

        $this->connection->executeStatement(
            <<<'SQL'
                INSERT INTO orders (order_id, status, previous_status)
                VALUES (:order_id, :status, :previous_status) AS incoming
                ON DUPLICATE KEY UPDATE
                    status          = incoming.status,
                    previous_status = incoming.previous_status
                SQL,
            [
                'order_id' => $orderId,
                'status' => is_string($status) ? $status : null,
                'previous_status' => is_string($previous) ? $previous : null,
            ],
        );
    }

    /**
     * @param array<string, mixed> $payload
     */
    private function cancelled(string $orderId, array $payload): void
    {
        $reason = $payload['reason'] ?? null;

        $this->connection->executeStatement(
            <<<'SQL'
                INSERT INTO orders (order_id, status, cancelled_reason)
                VALUES (:order_id, 'cancelled', :cancelled_reason) AS incoming
                ON DUPLICATE KEY UPDATE
                    status           = 'cancelled',
                    cancelled_reason = incoming.cancelled_reason
                SQL,
            [
                'order_id' => $orderId,
                'cancelled_reason' => is_string($reason) ? $reason : null,
            ],
        );
    }
}
