<?php

declare(strict_types=1);

namespace Workshop\Tests\App\Consumer;

use Doctrine\DBAL\Connection;
use PHPUnit\Framework\TestCase;
use Workshop\App\Consumer\CustomerRefDto;
use Workshop\App\Consumer\MoneyDto;
use Workshop\App\Consumer\OrderCreatedDto;
use Workshop\App\Consumer\OrderCreatedHandler;
use Workshop\App\Consumer\OrderTotalsDto;

final class OrderCreatedHandlerTest extends TestCase
{
    public function testUpsertsTheOrderRow(): void
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

        new OrderCreatedHandler($connection)(
            new OrderCreatedDto('ord-1', new CustomerRefDto('Jane'), new OrderTotalsDto(new MoneyDto(500))),
        );
    }
}
