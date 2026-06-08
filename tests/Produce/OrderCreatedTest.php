<?php

declare(strict_types=1);

namespace Workshop\Tests\Produce;

use PHPUnit\Framework\TestCase;
use Workshop\Produce\OrderCreated;

final class OrderCreatedTest extends TestCase
{
    public function testNamePartitionKeyAndPayload(): void
    {
        $message = new OrderCreated('ord-123');

        self::assertSame('order-created', $message->name());
        self::assertSame('ord-123', $message->partitionKey());

        $payload = $message->toPayload();
        self::assertSame('ord-123', $payload['order_id']);
        self::assertIsArray($payload['customer']);
        self::assertIsArray($payload['totals']);

        $totals = $payload['totals'];
        self::assertIsArray($totals['total']);
        self::assertSame(8606, $totals['total']['amount_cents']);
        self::assertArrayNotHasKey('metadata', $payload);
    }
}
