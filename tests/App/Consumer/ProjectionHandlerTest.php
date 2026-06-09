<?php

declare(strict_types=1);

namespace Workshop\Tests\App\Consumer;

use Doctrine\DBAL\Connection;
use PHPUnit\Framework\TestCase;
use Workshop\App\Consumer\CustomerRefDto;
use Workshop\App\Consumer\MoneyDto;
use Workshop\App\Consumer\OrderCancelledDto;
use Workshop\App\Consumer\OrderCreatedDto;
use Workshop\App\Consumer\OrderTotalsDto;
use Workshop\App\Consumer\OrderUpdatedDto;
use Workshop\App\Consumer\ProjectionHandler;

final class ProjectionHandlerTest extends TestCase
{
    public function testCreatedUpsertsTheOrderRow(): void
    {
        $connection = $this->createMock(Connection::class);
        $connection->expects(self::once())
            ->method('executeStatement')
            ->with(
                self::stringContains('INSERT INTO orders'),
                [
                    'order_id' => 'ord-1',
                    'customer_name' => 'Jane',
                    'total_cents' => 500,
                ],
            );

        new ProjectionHandler($connection)->handle(
            new OrderCreatedDto('ord-1', new CustomerRefDto('Jane'), new OrderTotalsDto(new MoneyDto(500))),
        );
    }

    public function testUpdatedRecordsTheStatusTransition(): void
    {
        $connection = $this->createMock(Connection::class);
        $connection->expects(self::once())
            ->method('executeStatement')
            ->with(
                self::stringContains('previous_status'),
                [
                    'order_id' => 'ord-2',
                    'status' => 'shipped',
                    'previous_status' => 'paid',
                ],
            );

        new ProjectionHandler($connection)->handle(new OrderUpdatedDto('ord-2', 'shipped', 'paid'));
    }

    public function testCancelledMarksTheOrderCancelled(): void
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

        new ProjectionHandler($connection)->handle(new OrderCancelledDto('ord-3', 'out of stock'));
    }
}
