<?php

declare(strict_types=1);

namespace Workshop\Tests\App\Consumer;

use Doctrine\DBAL\Connection;
use PHPUnit\Framework\TestCase;
use Workshop\App\Consumer\OrderUpdatedDto;
use Workshop\App\Consumer\OrderUpdatedHandler;

final class OrderUpdatedHandlerTest extends TestCase
{
    public function testRecordsTheStatusTransition(): void
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

        new OrderUpdatedHandler($connection)(new OrderUpdatedDto('ord-2', 'shipped', 'paid'));
    }
}
