<?php

declare(strict_types=1);

namespace Workshop\App\Producer;

#[MessageName('inventory.reserved')]
final class InventoryReserved extends Message
{
    public static function create(string $orderId): self
    {
        return new self($orderId, [
            'reservation_id' => self::generateId('rsv'),
            'order_id' => $orderId,
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
        ]);
    }
}
