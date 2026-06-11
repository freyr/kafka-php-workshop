<?php

declare(strict_types=1);

namespace Workshop\Framework\Db;

use Doctrine\DBAL\Connection;

/**
 * Provisions BOTH sides of the Block 9 projection demo, idempotently
 * (CREATE TABLE IF NOT EXISTS). Two bounded contexts share the workshop
 * database for demo convenience — in production each would own its schema:
 *
 *  - product_catalog_state_change (ProductCatalog context) — the Debezium
 *    source. Outbox-shaped (the EventRouter contract: id, aggregate_type,
 *    aggregate_id, event_type, payload, created_at) but append-only with NO
 *    published_at / poll cursor: the binlog is the only relay. MEDIUMBLOB
 *    payload — this lane is Confluent-framed AVRO only, there is no JSON
 *    flavor and no format switch.
 *
 *  - products_projection (Loyalty context) — the JDBC sink target. PRE-CREATED
 *    here because the sink runs schema.evolution=none: the demo controls the
 *    column types and the timestamp pair. created_at fills on first insert;
 *    updated_at re-stamps on every upsert that actually changes a value
 *    (MySQL's ON UPDATE fires only when row data changes).
 *    There are deliberately NO metadata columns: the sink connector drops the
 *    event's nested metadata record with a ReplaceField SMT before mapping
 *    fields to columns 1:1 — without that SMT the sink task would fail on the
 *    unmapped struct.
 *
 * created_at is DATETIME(3), not TIMESTAMP(3): Debezium maps DATETIME to
 * epoch-millis INT64, which the EventRouter's event.timestamp field requires
 * (same constraint documented on OutboxSchemaInstaller).
 */
final readonly class CatalogSchemaInstaller
{
    public function __construct(
        private Connection $connection,
    ) {
    }

    /**
     * @return list<string> the tables dropped, for the command to report
     */
    public function drop(): array
    {
        $this->connection->executeStatement('DROP TABLE IF EXISTS product_catalog_state_change');
        $this->connection->executeStatement('DROP TABLE IF EXISTS products_projection');

        return ['product_catalog_state_change', 'products_projection'];
    }

    /**
     * @return list<string> the tables ensured, for the command to report
     */
    public function install(): array
    {
        $this->connection->executeStatement(<<<'SQL'
            CREATE TABLE IF NOT EXISTS product_catalog_state_change (
              position       BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
              id             VARCHAR(36)  NOT NULL,
              aggregate_type VARCHAR(64)  NOT NULL,
              aggregate_id   VARCHAR(64)  NOT NULL,
              event_type     VARCHAR(64)  NOT NULL,
              payload        MEDIUMBLOB   NOT NULL,
              created_at     DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
              PRIMARY KEY (position),
              UNIQUE KEY uq_catalog_state_change_event (id)
            ) ENGINE=InnoDB
            SQL);

        $this->connection->executeStatement(<<<'SQL'
            CREATE TABLE IF NOT EXISTS products_projection (
              sku        VARCHAR(64)  NOT NULL,
              name       VARCHAR(255) NOT NULL,
              price      BIGINT       NOT NULL,
              margin     BIGINT       NOT NULL,
              created_at DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
              updated_at DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
              PRIMARY KEY (sku)
            ) ENGINE=InnoDB
            SQL);

        return ['product_catalog_state_change', 'products_projection'];
    }
}
