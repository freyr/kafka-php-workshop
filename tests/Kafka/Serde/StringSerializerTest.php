<?php

declare(strict_types=1);

namespace Workshop\Tests\Kafka\Serde;

use PHPUnit\Framework\TestCase;
use Workshop\Kafka\Serde\StringSerializer;

final class StringSerializerTest extends TestCase
{
    public function testRoundTripsAStringUnchanged(): void
    {
        $serializer = new StringSerializer();

        self::assertSame('order-placed-7', $serializer->decode($serializer->encode('order-placed-7')));
    }

    public function testDecodeReturnsBytesVerbatim(): void
    {
        $serializer = new StringSerializer();

        self::assertSame("raw\x00bytes", $serializer->decode("raw\x00bytes"));
    }
}
