<?php

declare(strict_types=1);

namespace Workshop\Produce;

#[MessageName('order-cancelled')]
final class OrderCancelled extends Message
{
    public function __construct(
        private readonly string $orderId,
        private readonly string $reason = 'CUSTOMER_REQUEST',
    ) {
        parent::__construct();
    }

    public function partitionKey(): string
    {
        return $this->orderId;
    }

    public function toPayload(): array
    {
        return [
            'order_id' => $this->orderId,
            'reason' => $this->reason,
            'cancelled_by' => null,
            'refund_amount' => null,
            'cancelled_at' => self::nowMillis(),
        ];
    }
}
