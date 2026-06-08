<?php

declare(strict_types=1);

namespace Workshop\Kafka\Serde;

use Workshop\Produce\Message;

/**
 * The seam between a typed Message and the bytes on the wire. Blocks 1-2 use the
 * JsonSerializer (native Symfony Serializer → JSON); Block 3 swaps in the
 * AvroSerializer; Block 4 will swap schema-versioned AVRO behind this same
 * interface — the producer, consumer, and run-loop never change. encode() takes
 * the whole Message: each serializer builds the envelope (and, for AVRO, resolves
 * the subject/schema) itself, so the producer stays oblivious to wire shape. The
 * message key (the producer's partition key) stays a transport concern, kept out
 * of encode().
 */
interface MessageSerializer
{
    public function encode(Message $payload): string;

    public function decode(string $bytes): mixed;
}
