<?php

declare(strict_types=1);

namespace Workshop\Tests\App\Catalog;

use Doctrine\DBAL\Connection;
use PHPUnit\Framework\TestCase;
use Workshop\App\Catalog\CatalogChangePlacer;
use Workshop\App\Catalog\CatalogStateChangeRepository;
use Workshop\App\Catalog\ProjectionChange;
use Workshop\App\Producer\Message;
use Workshop\App\Producer\MessageNameResolver;
use Workshop\Kafka\Serde\MessageSerializer;

/**
 * The placer's collaborators are real (they share the one mocked Connection), so
 * the test asserts at the SQL boundary: the wire bytes the serializer produced —
 * not a JSON re-encoding — land in the state-change row, inside transactional().
 */
final class CatalogChangePlacerTest extends TestCase
{
    public function testStoresTheSerializerBytesInsideTheTransaction(): void
    {
        $statements = [];
        $insideTransaction = false;

        $connection = $this->createMock(Connection::class);
        $connection->expects(self::once())
            ->method('transactional')
            ->willReturnCallback(static function (\Closure $func) use (&$insideTransaction): mixed {
                $insideTransaction = true;
                $result = $func();
                $insideTransaction = false;

                return $result;
            });
        $connection->method('executeStatement')
            ->willReturnCallback(function (string $sql, array $params = []) use (&$statements, &$insideTransaction): int {
                self::assertTrue($insideTransaction, 'the state-change INSERT must run inside transactional()');
                $statements[] = [$sql, $params];

                return 1;
            });

        $change = ProjectionChange::create('SKU-ESPRESSO-1KG', 'Espresso Beans 1kg', 4990, 1250);

        $this->placer($connection)->place($change);

        self::assertCount(1, $statements);
        self::assertStringContainsString('INSERT INTO product_catalog_state_change', $statements[0][0]);
        self::assertSame($change->eventId(), $statements[0][1]['id']);
        self::assertSame($change->partitionKey(), $statements[0][1]['aggregate_id']);
        self::assertSame('catalog.projection_change', $statements[0][1]['event_type']);
        self::assertSame("\x00FRAMED-AVRO", $statements[0][1]['payload'], 'the stored payload is the exact Confluent-framed wire bytes');
    }

    private function placer(Connection $connection): CatalogChangePlacer
    {
        $serializer = new class implements MessageSerializer {
            public function encode(Message $payload): string
            {
                return "\x00FRAMED-AVRO";
            }

            public function decode(string $bytes, ?\AvroSchema $readerSchema = null): mixed
            {
                return null;
            }
        };

        return new CatalogChangePlacer(
            $connection,
            new CatalogStateChangeRepository($connection),
            new MessageNameResolver(),
            $serializer,
        );
    }
}
