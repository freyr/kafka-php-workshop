<?php

declare(strict_types=1);

namespace Workshop\Tests\App\Outbox;

use Doctrine\DBAL\Connection;
use PHPUnit\Framework\TestCase;
use Workshop\App\Outbox\OrderStateWriter;
use Workshop\App\Producer\OrderCancelled;
use Workshop\App\Producer\OrderCreated;
use Workshop\App\Producer\OrderUpdated;

/**
 * The state writer applies a real message's payload, so each case feeds it the
 * payload of the catalog event it simulates — the same fixtures the AVRO path
 * produces — and asserts the orders upsert mirrors the consume-side handler.
 */
final class OrderStateWriterTest extends TestCase
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
                    'customer_name' => 'Jan Kowalski',
                    'total_cents' => 8606,
                ],
            );

        new OrderStateWriter($connection)->apply('order.created', 'ord-1', OrderCreated::create('ord-1')->payload);
    }

    public function testUpdatedRecordsTheStatusTransition(): void
    {
        $connection = $this->createMock(Connection::class);
        $connection->expects(self::once())
            ->method('executeStatement')
            ->with(
                self::stringContains('INSERT INTO orders'),
                [
                    'order_id' => 'ord-1',
                    'status' => 'PAID',
                    'previous_status' => 'CREATED',
                ],
            );

        new OrderStateWriter($connection)->apply('order.updated', 'ord-1', OrderUpdated::create('ord-1')->payload);
    }

    public function testCancelledMarksTheOrderWithItsReason(): void
    {
        $connection = $this->createMock(Connection::class);
        $connection->expects(self::once())
            ->method('executeStatement')
            ->with(
                self::stringContains('INSERT INTO orders'),
                [
                    'order_id' => 'ord-1',
                    'cancelled_reason' => 'CUSTOMER_REQUEST',
                ],
            );

        new OrderStateWriter($connection)->apply('order.cancelled', 'ord-1', OrderCancelled::create('ord-1')->payload);
    }

    public function testAnEventWithoutAStateChangeIsRejected(): void
    {
        $connection = $this->createMock(Connection::class);
        $connection->expects(self::never())->method('executeStatement');

        $this->expectException(\InvalidArgumentException::class);

        new OrderStateWriter($connection)->apply('order.audited', 'ord-1', []);
    }
}
