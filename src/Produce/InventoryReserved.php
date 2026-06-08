<?php

declare(strict_types=1);

namespace Workshop\Produce;

#[MessageName('inventory-reserved')]
final class InventoryReserved extends Message
{
    private readonly string $reservationId;

    public function __construct(
        private readonly string $orderId,
    ) {
        parent::__construct();
        $this->reservationId = self::generateId('rsv');
    }

    public function partitionKey(): string
    {
        return $this->orderId;
    }

    public function toPayload(): array
    {
        return [
            'reservation_id' => $this->reservationId,
            'order_id' => $this->orderId,
            'reserved_items' => [
                [
                    'product_id' => 'prod-555',
                    'sku' => 'TSHIRT-BLU-L',
                    'quantity' => 2,
                    'warehouse_id' => 'wh-waw-01',
                    'warehouse_location' => 'A-12-3',
                ],
            ],
            'reserved_at' => self::nowMillis(),
            'expires_at' => self::nowMillis() + 900_000,
        ];
    }
}
