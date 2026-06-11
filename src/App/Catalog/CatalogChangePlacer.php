<?php

declare(strict_types=1);

namespace Workshop\App\Catalog;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Exception\DriverException;
use FlixTech\SchemaRegistryApi\Exception\SchemaRegistryException;
use Workshop\App\Producer\MessageNameResolver;
use Workshop\Kafka\Serde\MessageSerializer;

/**
 * The produce side of the Block 9 projection demo, AVRO-only by design: the
 * payload column stores the exact Confluent-framed wire bytes, validated against
 * the registered subject at write time (the registry is the contract gate — the
 * "schema registered for anybody who wants to consume" promise). Debezium never
 * decodes them (ByteArray pass-through); the JDBC sink at the far end is the
 * first reader.
 *
 * In a real ProductCatalog context the product row's own UPDATE would sit inside
 * this transaction — the transactional-outbox discipline (Block 6). The demo has
 * no product table on purpose: the state-change log IS the published contract,
 * and the projection's correctness is checked against it.
 */
final readonly class CatalogChangePlacer
{
    public function __construct(
        private Connection $connection,
        private CatalogStateChangeRepository $stateChanges,
        private MessageNameResolver $names,
        private MessageSerializer $serializer,
    ) {
    }

    /**
     * @throws SchemaRegistryException when the subject is not registered yet
     * @throws DriverException         when the table is not provisioned (catalog:setup)
     */
    public function place(ProjectionChange $change): void
    {
        // Encode OUTSIDE the transaction: a registry cache miss is a remote call,
        // and a remote call has no business holding a DB transaction open.
        $payload = $this->serializer->encode($change);

        $this->connection->transactional(function () use ($change, $payload): void {
            $this->stateChanges->add(
                $change->eventId(),
                $change->partitionKey(),
                $this->names->nameOf($change),
                $payload,
            );
        });
    }
}
