<?php

declare(strict_types=1);

namespace Workshop\Tests\Kafka\Runtime;

use PHPUnit\Framework\TestCase;
use Workshop\Kafka\Runtime\OffsetReset;

final class OffsetResetTest extends TestCase
{
    public function testFromOptionParsesEveryCase(): void
    {
        self::assertSame(OffsetReset::Committed, OffsetReset::fromOption('committed'));
        self::assertSame(OffsetReset::Beginning, OffsetReset::fromOption('beginning'));
        self::assertSame(OffsetReset::End, OffsetReset::fromOption('end'));
    }

    public function testFromOptionRejectsUnknownValue(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Unknown offset reset "middle"');

        OffsetReset::fromOption('middle');
    }

    public function testCommittedDoesNotSeek(): void
    {
        self::assertNull(OffsetReset::Committed->seekOffset());
    }

    public function testBeginningAndEndSeekToTheLogEdges(): void
    {
        self::assertSame(RD_KAFKA_OFFSET_BEGINNING, OffsetReset::Beginning->seekOffset());
        self::assertSame(RD_KAFKA_OFFSET_END, OffsetReset::End->seekOffset());
    }
}
