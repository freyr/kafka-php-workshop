<?php

declare(strict_types=1);

namespace Workshop\Tests\Kafka\Serde;

use PHPUnit\Framework\TestCase;
use Workshop\Kafka\Serde\ConfluentFrame;

final class ConfluentFrameTest extends TestCase
{
    public function testRecognizesAFramedPayload(): void
    {
        self::assertTrue(ConfluentFrame::isFramed("\x00\x00\x00\x00\x06body"));
    }

    public function testRejectsBytesWithoutTheMagicByte(): void
    {
        self::assertFalse(ConfluentFrame::isFramed('raw avro body'));
    }

    public function testRejectsBytesShorterThanTheFrame(): void
    {
        self::assertFalse(ConfluentFrame::isFramed("\x00\x00\x00"));
    }

    public function testPrependBuildsMagicBytePlusBigEndianSchemaId(): void
    {
        $framed = ConfluentFrame::prepend(6, 'body');

        self::assertSame("\x00\x00\x00\x00\x06body", $framed);
        self::assertTrue(ConfluentFrame::isFramed($framed));
    }

    public function testPrependRoundTripsALargeSchemaId(): void
    {
        $framed = ConfluentFrame::prepend(65538, 'x');

        self::assertSame("\x00\x00\x01\x00\x02x", $framed);
    }
}
