<?php

declare(strict_types=1);

namespace Workshop\Tests\Consume;

use PHPUnit\Framework\TestCase;
use Workshop\Consume\DtoRouting;
use Workshop\Consume\OrderCreatedDto;

final class DtoRoutingTest extends TestCase
{
    public function testResolvesAndMisses(): void
    {
        $routing = new DtoRouting([
            'order-created' => OrderCreatedDto::class,
        ]);

        self::assertSame(OrderCreatedDto::class, $routing->for('order-created'));
        self::assertNull($routing->for('payment-processed'));
    }
}
