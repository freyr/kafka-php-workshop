<?php

declare(strict_types=1);

namespace Workshop\Tests\App\Consumer;

use PHPUnit\Framework\TestCase;
use Workshop\App\Consumer\MessageDenormalizer;
use Workshop\App\Consumer\OrderCreatedDto;

final class MessageDenormalizerTest extends TestCase
{
    public function testBuildsNestedDtoAndIgnoresUnknownKeys(): void
    {
        $payload = [
            'order_id' => 'ord-123',
            'customer' => [
                'customer_id' => 'cust-1',
                'email' => 'a@b.c',
                'display_name' => 'Jan Kowalski',
            ],
            'items' => [[
                'product_id' => 'p1',
                'quantity' => 2,
            ]],
            'totals' => [
                'subtotal' => [
                    'amount_cents' => 5998,
                    'currency' => 'PLN',
                ],
                'total' => [
                    'amount_cents' => 8606,
                    'currency' => 'PLN',
                ],
            ],
            'placed_at' => 1717761600000,
            'notes' => null,
        ];

        $dto = (new MessageDenormalizer())->denormalize($payload, OrderCreatedDto::class);

        self::assertInstanceOf(OrderCreatedDto::class, $dto);
        self::assertSame('ord-123', $dto->orderId);
        self::assertSame('Jan Kowalski', $dto->customer->displayName);
        self::assertSame(8606, $dto->totals->total->amountCents);
    }
}
