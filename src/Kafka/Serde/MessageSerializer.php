<?php

declare(strict_types=1);

namespace Workshop\Kafka\Serde;

/**
 * The seam between a PHP payload and the bytes on the wire. Blocks 1-2 use the
 * raw StringSerializer; Block 3 swaps in the AvroEnvelopeSerializer; Block 4 will
 * swap schema-versioned AVRO behind this same interface — the producer, consumer,
 * and run-loop never change. Value-only on purpose: the message key (the
 * producer's partition key) is the producer's concern, not the serializer's, so
 * AVRO never leaks into the base contract.
 */
interface MessageSerializer
{
    public function encode(mixed $payload): string;

    public function decode(string $bytes): mixed;
}
