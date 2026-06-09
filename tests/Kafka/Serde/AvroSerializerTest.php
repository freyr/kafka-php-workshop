<?php

declare(strict_types=1);

namespace Workshop\Tests\Kafka\Serde;

use FlixTech\AvroSerializer\Objects\RecordSerializer;
use PHPUnit\Framework\TestCase;
use Workshop\App\Producer\MessageNameResolver;
use Workshop\App\Producer\MessageRouting;
use Workshop\App\Producer\OrderCreated;
use Workshop\Kafka\Serde\AvroSerializer;

final class AvroSerializerTest extends TestCase
{
    private AvroSerializer $serializer;

    protected function setUp(): void
    {
        // The guard tests below never reach the wrapped RecordSerializer (non-AVRO
        // bytes short-circuit), so instantiate without the constructor — no Guzzle
        // client, no registry, no third-party deprecation.
        $this->serializer = (new \ReflectionClass(AvroSerializer::class))->newInstanceWithoutConstructor();
    }

    public function testDecodeReturnsNullForNonAvroBytes(): void
    {
        // The events:dispatch robustness contract: skip non-AVRO records.
        self::assertNull($this->serializer->decode('plain text, not avro'));
    }

    public function testDecodeReturnsNullForTooShortBuffer(): void
    {
        self::assertNull($this->serializer->decode("\x00\x00"));
    }

    public function testDecodeReturnsNullWhenMagicByteIsWrong(): void
    {
        // 5+ bytes but the leading byte is not the 0x00 Confluent magic byte.
        self::assertNull($this->serializer->decode("\x01\x00\x00\x00\x01payload"));
    }

    public function testEncodeThrowsForAnUnroutedMessage(): void
    {
        // The route is resolved before the RecordSerializer is touched, so an
        // unrouted message fails fast — no registry contact, stub serializer fine.
        $records = (new \ReflectionClass(RecordSerializer::class))->newInstanceWithoutConstructor();
        $serializer = new AvroSerializer($records, new MessageNameResolver(), new MessageRouting([]));

        $this->expectException(\InvalidArgumentException::class);

        $serializer->encode(OrderCreated::create('ord-1'));
    }
}
