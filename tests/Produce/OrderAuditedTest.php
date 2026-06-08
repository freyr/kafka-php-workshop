<?php

declare(strict_types=1);

namespace Workshop\Tests\Produce;

use PHPUnit\Framework\TestCase;
use Workshop\Produce\MessageNameResolver;
use Workshop\Produce\OrderAudited;

final class OrderAuditedTest extends TestCase
{
    public function testNamePartitionKeyAndPayload(): void
    {
        $message = OrderAudited::create('ord-123');

        self::assertSame('order.audited', (new MessageNameResolver())->nameOf($message));
        self::assertSame('ord-123', $message->partitionKey());

        $payload = $message->payload;
        self::assertSame('ord-123', $payload['order_id']);
        self::assertSame('observed', $payload['action']);
        self::assertArrayNotHasKey('metadata', $payload);
    }
}
