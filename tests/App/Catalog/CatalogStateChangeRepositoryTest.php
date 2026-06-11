<?php

declare(strict_types=1);

namespace Workshop\Tests\App\Catalog;

use Doctrine\DBAL\Connection;
use PHPUnit\Framework\TestCase;
use Workshop\App\Catalog\CatalogStateChangeRepository;

final class CatalogStateChangeRepositoryTest extends TestCase
{
    public function testAppendsOneStateChangeRowUnderTheProductCatalogAggregate(): void
    {
        $connection = $this->createMock(Connection::class);
        $connection->expects(self::once())
            ->method('executeStatement')
            ->with(
                self::stringContains('INSERT INTO product_catalog_state_change'),
                [
                    'id' => 'evt-1',
                    'aggregate_type' => 'ProductCatalog',
                    'aggregate_id' => 'SKU-ESPRESSO-1KG',
                    'event_type' => 'catalog.projection_change',
                    'payload' => "\x00WIRE-BYTES",
                ],
            );

        new CatalogStateChangeRepository($connection)
            ->add('evt-1', 'SKU-ESPRESSO-1KG', 'catalog.projection_change', "\x00WIRE-BYTES");
    }
}
