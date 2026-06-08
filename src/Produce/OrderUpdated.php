<?php

declare(strict_types=1);

namespace Workshop\Produce;

#[MessageName('order-updated')]
final class OrderUpdated extends Message
{
    public function __construct(
        private readonly string $orderId,
        private readonly string $status = 'PAID',
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
            'status' => $this->status,
            'previous_status' => 'CREATED',
            'updated_at' => self::nowMillis(),
            'note' => null,
        ];
    }
}
