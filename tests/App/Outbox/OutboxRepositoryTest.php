<?php

declare(strict_types=1);

namespace Workshop\Tests\App\Outbox;

use Doctrine\DBAL\ArrayParameterType;
use Doctrine\DBAL\Connection;
use PHPUnit\Framework\TestCase;
use Workshop\App\Outbox\OutboxRepository;

final class OutboxRepositoryTest extends TestCase
{
    public function testAddInsertsOneOutboxRow(): void
    {
        $connection = $this->createMock(Connection::class);
        $connection->expects(self::once())
            ->method('executeStatement')
            ->with(
                self::stringContains('INSERT INTO outbox'),
                [
                    'id' => 'evt-1',
                    'aggregate_type' => 'Order',
                    'aggregate_id' => 'ord-1',
                    'event_type' => 'order.created',
                    'payload' => '{"a":1}',
                ],
            );

        new OutboxRepository($connection)->add('evt-1', 'Order', 'ord-1', 'order.created', '{"a":1}');
    }

    public function testFetchUnpublishedMapsRowsToTypedRecords(): void
    {
        // PDO returns strings under emulated prepares; the repository narrows them.
        $connection = $this->createMock(Connection::class);
        $connection->expects(self::once())
            ->method('fetchAllAssociative')
            ->with(self::logicalAnd(
                self::stringContains('published_at IS NULL'),
                self::stringContains('ORDER BY position'),
                self::stringContains('LIMIT 5'),
            ))
            ->willReturn([
                [
                    'position' => '7',
                    'id' => 'evt-1',
                    'aggregate_type' => 'Order',
                    'aggregate_id' => 'ord-1',
                    'event_type' => 'order.created',
                    'payload' => '{"a":1}',
                ],
            ]);

        $records = new OutboxRepository($connection)->fetchUnpublished(5);

        self::assertCount(1, $records);
        self::assertSame(7, $records[0]->position);
        self::assertSame('evt-1', $records[0]->id);
        self::assertSame('Order', $records[0]->aggregateType);
        self::assertSame('ord-1', $records[0]->aggregateId);
        self::assertSame('order.created', $records[0]->eventType);
        self::assertSame('{"a":1}', $records[0]->payload);
    }

    public function testMarkPublishedStampsTheGivenPositions(): void
    {
        $connection = $this->createMock(Connection::class);
        $connection->expects(self::once())
            ->method('executeStatement')
            ->with(
                self::stringContains('SET published_at = NOW(3) WHERE position IN (?)'),
                [[3, 4]],
                [ArrayParameterType::INTEGER],
            )
            ->willReturn(2);

        self::assertSame(2, new OutboxRepository($connection)->markPublished([3, 4]));
    }

    public function testCountUnpublishedNarrowsTheScalar(): void
    {
        $connection = self::createStub(Connection::class);
        $connection->method('fetchOne')->willReturn('7');

        self::assertSame(7, new OutboxRepository($connection)->countUnpublished());
    }
}
