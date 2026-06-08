<?php

declare(strict_types=1);

namespace Workshop\Produce;

#[MessageName('order.updated')]
final class OrderUpdated extends Message
{
    public static function create(string $orderId, string $status = 'PAID'): self
    {
        return new self($orderId, [
            'order_id' => $orderId,
            'status' => $status,
            'previous_status' => 'CREATED',
            'updated_at' => self::nowMillis(),
            'note' => null,
        ]);
    }
}
