<?php

declare(strict_types=1);

namespace Workshop\Tests\Kafka\Callback;

use PHPUnit\Framework\TestCase;
use RdKafka\Message;
use Workshop\Kafka\Callback\DeliveryTally;

/**
 * \RdKafka\Message is a plain property bag, so each case fabricates the delivery
 * report librdkafka would hand the callback and feeds it through record().
 */
final class DeliveryTallyTest extends TestCase
{
    public function testCountsAcksAndFailuresSeparately(): void
    {
        $lines = [];
        $tally = new DeliveryTally(static function (string $line) use (&$lines): void {
            $lines[] = $line;
        });

        $acked = new Message();
        $acked->err = RD_KAFKA_RESP_ERR_NO_ERROR;
        $acked->partition = 2;
        $acked->offset = 42;

        $failed = new Message();
        $failed->err = RD_KAFKA_RESP_ERR__MSG_TIMED_OUT;
        $failed->partition = 0;
        $failed->offset = -1;

        $tally->record($acked);
        $tally->record($failed);

        self::assertSame(1, $tally->delivered());
        self::assertSame(1, $tally->failed());
        self::assertSame('✓ delivered partition=2 offset=42', $lines[0]);
        self::assertStringContainsString('✗ delivery failed', $lines[1]);
    }

    public function testResetStartsANewBatch(): void
    {
        $tally = new DeliveryTally();

        $failed = new Message();
        $failed->err = RD_KAFKA_RESP_ERR__MSG_TIMED_OUT;
        $failed->partition = 0;
        $failed->offset = -1;
        $tally->record($failed);

        $tally->reset();

        self::assertSame(0, $tally->delivered());
        self::assertSame(0, $tally->failed());
    }
}
