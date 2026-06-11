<?php

declare(strict_types=1);

namespace Workshop\Framework\Db;

use Doctrine\DBAL\Connection;

/**
 * Provisions the Block 6 outbox table idempotently (CREATE TABLE IF NOT EXISTS),
 * so `outbox:setup` can be re-run safely. The payload column is a binary blob:
 * the placer stores Confluent-framed AVRO bytes, fingerprinted by
 * payloadColumnType() so the setup command can refuse a table left behind by an
 * older provisioning. One generic table serves both relay flavors:
 *
 *  - the Debezium CDC relay tails the binlog for INSERTs and routes each row via
 *    the EventRouter SMT — its configured columns (id, aggregate_type,
 *    aggregate_id, event_type, payload, created_at) match
 *    config/debezium-outbox-connector.json exactly;
 *  - the PHP polling relay (`outbox:relay`) drains rows in `position` order and
 *    stamps `published_at` once the broker acked them. Debezium ignores those two
 *    extra columns — but the UPDATE that stamps published_at IS a binlog event, so
 *    run one relay flavor at a time or the connector logs a warning per mark.
 */
final readonly class OutboxSchemaInstaller
{
    /**
     * The payload column's MySQL data type, as information_schema reports it —
     * the fingerprint the setup command compares payloadColumnType() against to
     * detect a stale table that needs --fresh.
     */
    public const string PAYLOAD_COLUMN_TYPE = 'mediumblob';

    public function __construct(
        private Connection $connection,
    ) {
    }

    /**
     * Drops the outbox so the next install() starts from an empty store — the
     * reset path behind `outbox:setup --fresh`. Deliberately leaves the orders
     * projection alone; resetting that is `kafka:consume:setup --fresh`.
     *
     * @return list<string> the tables dropped, for the command to report
     */
    public function drop(): array
    {
        $this->connection->executeStatement('DROP TABLE IF EXISTS outbox');

        return ['outbox'];
    }

    /**
     * The current payload column's data type as MySQL reports it (e.g.
     * 'mediumblob', or 'json' from a pre-AVRO provisioning), or null when the
     * outbox does not exist yet. Compared against PAYLOAD_COLUMN_TYPE to detect
     * a stale table that needs --fresh.
     */
    public function payloadColumnType(): ?string
    {
        $type = $this->connection->fetchOne(<<<'SQL'
            SELECT DATA_TYPE FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'outbox' AND COLUMN_NAME = 'payload'
            SQL);

        return is_string($type) ? strtolower($type) : null;
    }

    /**
     * @return list<string> the tables ensured, for the command to report
     */
    public function install(): array
    {
        // position is the relay's poll cursor: AUTO_INCREMENT gives unpublished
        // rows a stable publish order without trusting timestamps. The covering
        // index lets `WHERE published_at IS NULL ORDER BY position` walk the
        // pending set without a filesort.
        //
        // created_at is DATETIME(3), not TIMESTAMP(3), on purpose: Debezium maps
        // DATETIME to epoch-millis INT64 (io.debezium.time.Timestamp), which is
        // what the EventRouter's event.timestamp field requires — TIMESTAMP maps
        // to a ZonedTimestamp STRING and kills the connector task with
        // "Field 'created_at' is not of type INT64".
        $this->connection->executeStatement(<<<'SQL'
            CREATE TABLE IF NOT EXISTS outbox (
              position       BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
              id             VARCHAR(36)  NOT NULL,
              aggregate_type VARCHAR(64)  NOT NULL,
              aggregate_id   VARCHAR(64)  NOT NULL,
              event_type     VARCHAR(64)  NOT NULL,
              payload        MEDIUMBLOB   NOT NULL,
              created_at     DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
              published_at   DATETIME(3)  NULL DEFAULT NULL,
              PRIMARY KEY (position),
              UNIQUE KEY uq_outbox_event (id),
              KEY ix_outbox_pending (published_at, position)
            ) ENGINE=InnoDB
            SQL);

        return ['outbox'];
    }
}
