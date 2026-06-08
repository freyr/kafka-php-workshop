<?php

declare(strict_types=1);

namespace Workshop\Tests\Kafka\Serde;

use PHPUnit\Framework\TestCase;
use Workshop\Kafka\Serde\AvroPayload;
use Workshop\Kafka\Serde\AvroSerializer;

final class AvroSerializerTest extends TestCase
{
    private AvroSerializer $serializer;

    protected function setUp(): void
    {
        // The guard tests below never reach the wrapped RecordSerializer (non-AVRO
        // bytes short-circuit, bad payloads throw first), so instantiate without the
        // constructor — no Guzzle client, no registry, no third-party deprecation.
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

    public function testEncodeRejectsANonAvroPayload(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        $this->serializer->encode('just a string');
    }

    public function testAvroPayloadCarriesSubjectSchemaAndRecord(): void
    {
        $payload = new AvroPayload('com.ecommerce.orders.v1.order_created', '{"type":"record"}', [
            'k' => 'v',
        ]);

        self::assertSame('com.ecommerce.orders.v1.order_created', $payload->subject);
        self::assertSame([
            'k' => 'v',
        ], $payload->record);
    }
}
