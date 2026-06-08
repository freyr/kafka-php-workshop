<?php

declare(strict_types=1);

namespace Workshop\Tests\Kafka\Serde;

use PHPUnit\Framework\TestCase;
use Workshop\Kafka\Serde\JsonSerializer;
use Workshop\Produce\TextMessage;

final class JsonSerializerTest extends TestCase
{
    public function testEncodesATypedMessageToLowerSnakeJson(): void
    {
        $serializer = new JsonSerializer();

        $json = $serializer->encode(new TextMessage(sequence: 1, key: 'a', text: 'event-1', timestamp: 1717840000123));

        self::assertJsonStringEqualsJsonString(
            '{"sequence":1,"key":"a","text":"event-1","timestamp":1717840000123}',
            $json,
        );
    }

    public function testEncodesANullKeyAsJsonNull(): void
    {
        $serializer = new JsonSerializer();

        $json = $serializer->encode(new TextMessage(sequence: 2, key: null, text: 'event-2', timestamp: 1717840000456));

        self::assertJsonStringEqualsJsonString(
            '{"sequence":2,"key":null,"text":"event-2","timestamp":1717840000456}',
            $json,
        );
    }

    public function testDecodesBackToAnAssociativeArray(): void
    {
        $serializer = new JsonSerializer();

        $decoded = $serializer->decode('{"sequence":1,"key":"a","text":"event-1","timestamp":1717840000123}');

        self::assertSame(
            [
                'sequence' => 1,
                'key' => 'a',
                'text' => 'event-1',
                'timestamp' => 1717840000123,
            ],
            $decoded,
        );
    }

    public function testRoundTripsAMessageThroughTheWire(): void
    {
        $serializer = new JsonSerializer();
        $message = new TextMessage(sequence: 7, key: 'order-7', text: 'order-placed', timestamp: 1717840000789);

        $decoded = $serializer->decode($serializer->encode($message));

        self::assertSame(
            [
                'sequence' => 7,
                'key' => 'order-7',
                'text' => 'order-placed',
                'timestamp' => 1717840000789,
            ],
            $decoded,
        );
    }
}
