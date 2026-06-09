<?php

declare(strict_types=1);

namespace Workshop\Tests\App\Consumer;

use Doctrine\DBAL\Connection;
use PHPUnit\Framework\TestCase;
use Workshop\App\Consumer\OrderCancelledDto;
use Workshop\App\Consumer\OrderCancelledHandler;

final class OrderCancelledHandlerTest extends TestCase
{
    public function testMarksTheOrderCancelled(): void
    {
        $connection = $this->createMock(Connection::class);
        $connection->expects(self::once())
            ->method('executeStatement')
            ->with(
                self::stringContains("'cancelled'"),
                [
                    'order_id' => 'ord-3',
                    'cancelled_reason' => 'out of stock',
                ],
            );

        new OrderCancelledHandler($connection)(new OrderCancelledDto('ord-3', 'out of stock'));
    }
}
