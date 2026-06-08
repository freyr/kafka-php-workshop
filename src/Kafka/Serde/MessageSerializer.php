<?php

declare(strict_types=1);

namespace Workshop\Kafka\Serde;

use Workshop\Produce\Message;

/**
 * The seam between a typed Message and the bytes on the wire. The AvroSerializer
 * implements it over the Confluent wire format; Block 4 will swap schema-versioned
 * AVRO behind this same interface — the producer, consumer, and run-loop never
 * change. encode() takes the whole Message: the serializer builds the envelope and
 * resolves the subject/schema itself, so the producer stays oblivious to wire
 * shape. The message key (the producer's partition key) stays a transport concern,
 * kept out of encode().
 */
interface MessageSerializer
{
    public function encode(Message $payload): string;

    public function decode(string $bytes): mixed;
}
