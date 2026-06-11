<?php

declare(strict_types=1);

namespace Workshop\Tests\Enqueue;

use Enqueue\RdKafka\RdKafkaMessage;
use PHPUnit\Framework\TestCase;
use Workshop\Enqueue\RawBodySerializer;

/**
 * The serializer must be a true pass-through: Confluent-framed AVRO starts with a
 * zero magic byte and a binary schema id, so any wrapping (enqueue's default JSON
 * envelope) or mangling would corrupt the frame for every other consumer.
 */
final class RawBodySerializerTest extends TestCase
{
    public function testToStringReturnsTheBodyVerbatim(): void
    {
        $framed = "\x00\x00\x00\x00\x07" . 'avro-bytes';

        $serialized = new RawBodySerializer()->toString(new RdKafkaMessage($framed));

        self::assertSame($framed, $serialized);
    }

    public function testToMessageWrapsTheBytesUntouched(): void
    {
        $framed = "\x00\x00\x00\x00\x07" . 'avro-bytes';

        $message = new RawBodySerializer()->toMessage($framed);

        self::assertSame($framed, $message->getBody());
        self::assertSame([], $message->getProperties());
        self::assertSame([], $message->getHeaders());
    }

    public function testRoundTripIsLossless(): void
    {
        $serializer = new RawBodySerializer();
        $body = random_bytes(64);

        self::assertSame($body, $serializer->toString($serializer->toMessage($body)));
    }
}
