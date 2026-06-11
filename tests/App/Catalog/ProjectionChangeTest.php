<?php

declare(strict_types=1);

namespace Workshop\Tests\App\Catalog;

use PHPUnit\Framework\TestCase;
use Workshop\App\Catalog\ProjectionChange;
use Workshop\App\Producer\MessageNameResolver;

final class ProjectionChangeTest extends TestCase
{
    public function testCarriesTheFullProductStateKeyedBySku(): void
    {
        $change = ProjectionChange::create('SKU-ESPRESSO-1KG', 'Espresso Beans 1kg', 4990, 1250);

        self::assertSame('SKU-ESPRESSO-1KG', $change->partitionKey(), 'sku is the partition key — all changes to one product stay in partition order');
        self::assertSame([
            'sku' => 'SKU-ESPRESSO-1KG',
            'name' => 'Espresso Beans 1kg',
            'price' => 4990,
            'margin' => 1250,
        ], $change->payload, 'the payload is the FULL state, flat — the JDBC sink maps fields to columns 1:1');
    }

    public function testEnvelopeWrapsThePayloadWithMetadata(): void
    {
        $change = ProjectionChange::create('SKU-ESPRESSO-1KG', 'Espresso Beans 1kg', 4990, 1250);

        $envelope = $change->envelope();

        self::assertIsArray($envelope['metadata']);
        self::assertSame($change->eventId(), $envelope['metadata']['event_id']);
        self::assertSame($change->payload, array_diff_key($envelope, [
            'metadata' => null,
        ]), 'the whole flat payload survives at the envelope top level — exactly the shape the AVRO schema declares');
    }

    public function testResolvesItsWireName(): void
    {
        $change = ProjectionChange::create('SKU-ESPRESSO-1KG', 'Espresso Beans 1kg', 4990, 1250);

        self::assertSame('catalog.projection_change', new MessageNameResolver()->nameOf($change));
    }
}
