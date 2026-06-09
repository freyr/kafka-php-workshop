<?php

declare(strict_types=1);

namespace Workshop\Tests\App\Producer;

use PHPUnit\Framework\TestCase;
use Workshop\App\Producer\MessageNameResolver;
use Workshop\App\Producer\OrderCreated;

final class OrderCreatedTest extends TestCase
{
    public function testNamePartitionKeyAndPayload(): void
    {
        $message = OrderCreated::create('ord-123');

        self::assertSame('order.created', (new MessageNameResolver())->nameOf($message));
        self::assertSame('ord-123', $message->partitionKey());

        $payload = $message->payload;
        self::assertSame('ord-123', $payload['order_id']);
        self::assertIsArray($payload['customer']);
        self::assertIsArray($payload['totals']);

        $totals = $payload['totals'];
        self::assertIsArray($totals['total']);
        self::assertSame(8606, $totals['total']['amount_cents']);
        self::assertArrayNotHasKey('metadata', $payload);
    }
}
