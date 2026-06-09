<?php

declare(strict_types=1);

namespace Workshop\Tests\App\Producer;

use PHPUnit\Framework\TestCase;
use Workshop\App\Producer\MessageNameResolver;
use Workshop\App\Producer\OrderAudited;

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
