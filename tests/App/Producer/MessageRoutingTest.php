<?php

declare(strict_types=1);

namespace Workshop\Tests\App\Producer;

use PHPUnit\Framework\TestCase;
use Workshop\App\Producer\MessageRouting;

final class MessageRoutingTest extends TestCase
{
    public function testResolvesAKnownName(): void
    {
        $routing = new MessageRouting([
            'order.created' => [
                'topic' => 'enet.ecommerce.orders',
                'subject' => 'com.ecommerce.orders.OrderCreated',
                'schema' => __DIR__ . '/../../../schemas/orders/OrderCreated.avsc',
            ],
        ]);

        $route = $routing->for('order.created');

        self::assertSame('enet.ecommerce.orders', $route->topic);
        self::assertSame('com.ecommerce.orders.OrderCreated', $route->subject);
        self::assertStringContainsString('"OrderCreated"', $route->schemaJson());
    }

    public function testThrowsOnUnknownName(): void
    {
        $routing = new MessageRouting([]);

        $this->expectException(\InvalidArgumentException::class);
        $routing->for('nope');
    }
}
