<?php

declare(strict_types=1);

namespace Workshop\Kafka\Serde;

use FlixTech\AvroSerializer\Objects\RecordSerializer;
use FlixTech\SchemaRegistryApi\Exception\SchemaRegistryException;
use Workshop\App\Producer\Message;
use Workshop\App\Producer\MessageNameResolver;
use Workshop\App\Producer\MessageRouting;

/**
 * Block 3 serializer: the MessageSerializer seam over the Confluent AVRO wire
 * format (a 0x00 magic byte + 4-byte big-endian schema id + AVRO binary).
 *
 * The Schema Registry plumbing — Guzzle client → registry → RecordSerializer — is
 * assembled as data in config/services.yaml and injected ready-made. encode()
 * takes a Message and does the AVRO framing itself: it resolves the message's wire
 * name, looks up the route (subject + schema), and encodes the enveloped record.
 * Schemas are NOT auto-registered: the registry stays a strict gate, so a subject
 * must be registered out of band (bin/console kafka:schema:register) before its messages
 * can be produced — encode throws a SchemaRegistryException otherwise. decode()
 * returns the structured envelope, or null when the bytes are not Confluent-framed
 * so a dispatcher can skip non-AVRO records instead of crashing.
 */
final readonly class AvroSerializer implements MessageSerializer
{
    /**
     * Confluent wire format: a 0x00 magic byte, a 4-byte big-endian schema id,
     * then the AVRO body — so a framed message is at least 5 bytes.
     */
    private const string MAGIC_BYTE = "\x00";
    private const int HEADER_BYTES = 5;

    public function __construct(
        private RecordSerializer $serializer,
        private MessageNameResolver $names,
        private MessageRouting $routing,
    ) {
    }

    /**
     * @throws \AvroSchemaParseException
     * @throws SchemaRegistryException
     */
    public function encode(Message $payload): string
    {
        // Resolve the route before touching the RecordSerializer, so an unrouted
        // message fails fast (and without any registry contact).
        $name = $this->names->nameOf($payload);
        $route = $this->routing->for($name);

        // No conformance guard on purpose: the Avro writer substitutes the schema
        // default for any field the payload omits. That leniency is the Block 4
        // lesson — a producer left behind by a schema change ships the default
        // silently, the registry never complains, and the drift only surfaces when
        // you read the data back. The exercise has you experience that, prod-style.
        return $this->serializer->encodeRecord($route->subject, \AvroSchema::parse($route->schemaJson()), $payload->envelope());
    }

    /**
     * Decode a Confluent-framed message back to the structured envelope, or null
     * for bytes that are not Confluent-framed — the events:dispatch robustness
     * contract: skip records you cannot decode instead of crashing.
     *
     * With no reader schema the message is decoded with its own *writer* schema —
     * the one pinned by the schema id in the bytes — so the structure comes back
     * exactly as produced (an old message returns in its old shape, missing fields
     * added later). Pass a $readerSchema to resolve writer→reader instead: Avro
     * fills reader fields the writer lacked from their defaults, so a mixed-version
     * stream reads back in one uniform shape. A genuine decode failure throws;
     * route that poison message to a DLQ.
     *
     * @return array<string, mixed>|null
     *
     * @throws SchemaRegistryException
     */
    public function decode(string $bytes, ?\AvroSchema $readerSchema = null): mixed
    {
        if (! $this->isConfluentFramed($bytes)) {
            return null;
        }

        /** @var array<string, mixed> $decoded */
        $decoded = $this->serializer->decodeMessage($bytes, $readerSchema);

        return $decoded;
    }

    private function isConfluentFramed(string $bytes): bool
    {
        return strlen($bytes) >= self::HEADER_BYTES && self::MAGIC_BYTE === $bytes[0];
    }
}
