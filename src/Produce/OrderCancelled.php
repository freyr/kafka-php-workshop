<?php

declare(strict_types=1);

namespace Workshop\Produce;

#[MessageName('order.cancelled')]
final class OrderCancelled extends Message
{
    public static function create(string $orderId, string $reason = 'CUSTOMER_REQUEST'): self
    {
        return new self($orderId, [
            'order_id' => $orderId,
            'reason' => $reason,
            'cancelled_by' => null,
            'refund_amount' => null,
            'cancelled_at' => self::nowMillis(),
        ]);
    }
}
