<?php

declare(strict_types=1);

namespace Workshop\App\Outbox;

/**
 * How the outbox stores (and the relays carry) the event payload. The format is a
 * provisioning-time decision — the payload column's type — and the place command
 * must encode to match:
 *
 *  - Json: a human-readable JSON envelope in a JSON column. Debezium's
 *    EventRouter expands it (expand.json.payload) and the topic carries JSON.
 *  - Avro: the app serializes the envelope to Confluent-framed AVRO bytes (magic
 *    byte + registered schema id) via the same AvroSerializer the direct produce
 *    path uses, stored in a binary column. Both relays then move opaque bytes,
 *    and the topic carries records byte-identical to kafka:produce:sample's —
 *    same schemas, same registry governance, decodable by kafka:consume.
 */
enum PayloadFormat: string
{
    case Json = 'json';
    case Avro = 'avro';

    public static function fromOption(string $option): self
    {
        return self::tryFrom($option)
            ?? throw new \InvalidArgumentException(sprintf('Unknown format: %s (use: json | avro)', $option));
    }

    /**
     * The payload column's MySQL data type, as information_schema reports it —
     * both the DDL the installer emits and the fingerprint the setup command uses
     * to detect a format switch that needs --fresh.
     */
    public function columnType(): string
    {
        return match ($this) {
            self::Json => 'json',
            self::Avro => 'mediumblob',
        };
    }
}
