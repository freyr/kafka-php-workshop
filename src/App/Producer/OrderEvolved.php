<?php

declare(strict_types=1);

namespace Workshop\App\Producer;

/**
 * The Block 4 schema-evolution playground event — deliberately flat (order_id,
 * customer_name, amount_cents) so the consumer prints one field per line. You
 * evolve this event in place during the exercise: add a field to
 * schemas/demo/OrderEvolved.avsc, register it, then add it here so the producer
 * actually populates it. It is isolated on enet.demo.orders, so evolving it never
 * touches the real OrderCreated used by the other blocks.
 */
#[MessageName('demo.order.evolved')]
final class OrderEvolved extends Message
{
    public static function create(string $orderId): self
    {
        return new self($orderId, [
            'order_id' => $orderId,
            'customer_name' => 'Jan Kowalski',
            'amount_cents' => 8606,
        ]);
    }
}
