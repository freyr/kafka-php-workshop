<?php

declare(strict_types=1);

namespace Workshop\Tests\Kafka\Serde;

use PHPUnit\Framework\TestCase;
use Workshop\Kafka\Serde\JsonSerializer;
use Workshop\Produce\MessageNameResolver;
use Workshop\Produce\TextMessage;

final class JsonSerializerTest extends TestCase
{
    private JsonSerializer $serializer;

    protected function setUp(): void
    {
        $this->serializer = new JsonSerializer(new MessageNameResolver());
    }

    public function testEncodesAMessageToItsEnvelopedJson(): void
    {
        $json = $this->serializer->encode(TextMessage::create(1, 'a', 'event-1'));

        /** @var array<string, mixed> $decoded */
        $decoded = json_decode($json, true, flags: JSON_THROW_ON_ERROR);

        // The business payload travels flat alongside the metadata envelope.
        self::assertSame([
            'sequence' => 1,
            'key' => 'a',
            'text' => 'event-1',
        ], array_diff_key($decoded, [
            'metadata' => null,
        ]));

        self::assertIsArray($decoded['metadata']);
        /** @var array<string, mixed> $metadata */
        $metadata = $decoded['metadata'];
        self::assertSame(['event_id', 'timestamp', 'name'], array_keys($metadata));
        self::assertSame('text', $metadata['name']);
        self::assertIsInt($metadata['timestamp']);
    }

    public function testEncodesANullKeyAsJsonNull(): void
    {
        $json = $this->serializer->encode(TextMessage::create(2, null, 'event-2'));

        /** @var array<string, mixed> $decoded */
        $decoded = json_decode($json, true, flags: JSON_THROW_ON_ERROR);

        self::assertNull($decoded['key']);
    }

    public function testDecodesBackToAnAssociativeArray(): void
    {
        $decoded = $this->serializer->decode(
            '{"metadata":{"event_id":"x","timestamp":1717840000123,"name":"text"},"sequence":1,"key":"a","text":"event-1"}',
        );

        self::assertSame([
            'metadata' => [
                'event_id' => 'x',
                'timestamp' => 1717840000123,
                'name' => 'text',
            ],
            'sequence' => 1,
            'key' => 'a',
            'text' => 'event-1',
        ], $decoded);
    }

    public function testRoundTripsAMessageThroughTheWire(): void
    {
        $decoded = $this->serializer->decode($this->serializer->encode(TextMessage::create(7, 'order-7', 'order-placed')));

        self::assertIsArray($decoded);
        self::assertSame([
            'sequence' => 7,
            'key' => 'order-7',
            'text' => 'order-placed',
        ], array_diff_key($decoded, [
            'metadata' => null,
        ]));
    }
}
