<?php

declare(strict_types=1);

namespace Workshop\Kafka\Serde;

/**
 * The Confluent wire frame, as a value: a 0x00 magic byte + a 4-byte big-endian
 * schema id, prepended to the AVRO body. AvroSerializer consults it on every
 * decode; the DLQ repair path uses prepend() to re-frame a payload that shipped
 * as raw AVRO (the wrong-serializer poison) once the operator has identified
 * the writer schema.
 */
final readonly class ConfluentFrame
{
    public const string MAGIC_BYTE = "\x00";
    public const int HEADER_BYTES = 5;

    public static function isFramed(string $bytes): bool
    {
        return \strlen($bytes) >= self::HEADER_BYTES && str_starts_with($bytes, self::MAGIC_BYTE);
    }

    public static function prepend(int $schemaId, string $body): string
    {
        return self::MAGIC_BYTE . pack('N', $schemaId) . $body;
    }
}
