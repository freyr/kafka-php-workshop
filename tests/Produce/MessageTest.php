<?php

declare(strict_types=1);

namespace Workshop\Tests\Produce;

use PHPUnit\Framework\TestCase;
use Workshop\Produce\Message;
use Workshop\Produce\MessageName;

final class MessageTest extends TestCase
{
    public function testMessageNameAttributeCarriesValue(): void
    {
        $attribute = new MessageName('order.created');

        self::assertSame('order.created', $attribute->value);
    }

    public function testEnvelopeCarriesMetadataAndFlatPayload(): void
    {
        $message = MessageTestDouble::create('ord-1');

        $envelope = $message->envelope();

        self::assertSame([
            'order_id' => 'ord-1',
            'status' => 'NEW',
        ], array_diff_key($envelope, [
            'metadata' => null,
        ]));
        self::assertIsArray($envelope['metadata']);
        /** @var array<string, mixed> $metadata */
        $metadata = $envelope['metadata'];
        // The wire name is not in the envelope — it rides as the message-name header.
        self::assertSame(['event_id', 'timestamp'], array_keys($metadata));
        self::assertIsInt($metadata['timestamp']);
        self::assertGreaterThan(1_700_000_000_000, $metadata['timestamp']);
        self::assertIsString($metadata['event_id']);
        self::assertMatchesRegularExpression('/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/', $metadata['event_id']);
        self::assertSame('ord-1', $message->partitionKey());
    }

    public function testRejectsAReservedMetadataKeyInPayload(): void
    {
        $this->expectException(\LogicException::class);

        // The metadata key is reserved by the envelope, so a payload carrying it
        // is rejected at construction.
        new class extends Message {
            public function __construct()
            {
                parent::__construct('x', [
                    'metadata' => 'oops',
                ]);
            }
        };
    }
}
