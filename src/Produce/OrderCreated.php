<?php

declare(strict_types=1);

namespace Workshop\Produce;

#[MessageName('order-created')]
final class OrderCreated extends Message
{
    public function __construct(
        private readonly string $orderId,
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
            'customer' => [
                'customer_id' => 'cust-9876',
                'email' => 'jan@example.com',
                'display_name' => 'Jan Kowalski',
            ],
            'items' => [
                [
                    'product_id' => 'prod-555',
                    'sku' => 'TSHIRT-BLU-L',
                    'product_name' => 'Blue T-Shirt Large',
                    'quantity' => 2,
                    'unit_price' => self::money(2999),
                    'line_total' => self::money(5998),
                ],
            ],
            'shipping_address' => [
                'street' => 'ul. Marszalkowska 1',
                'city' => 'Warszawa',
                'postal_code' => '00-001',
                'country' => 'PL',
                'state' => null,
            ],
            'totals' => [
                'subtotal' => self::money(5998),
                'shipping_cost' => self::money(999),
                'tax' => self::money(1609),
                'total' => self::money(8606),
            ],
            'placed_at' => self::nowMillis(),
            'notes' => null,
        ];
    }
}
