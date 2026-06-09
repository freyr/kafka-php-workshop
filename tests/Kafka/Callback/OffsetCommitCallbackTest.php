<?php

declare(strict_types=1);

namespace Workshop\Tests\Kafka\Callback;

use PHPUnit\Framework\TestCase;
use RdKafka\TopicPartition;
use Workshop\Kafka\Callback\OffsetCommitCallback;

final class OffsetCommitCallbackTest extends TestCase
{
    public function testSuccessfulCommitIsSilent(): void
    {
        self::assertNull($this->describe(RD_KAFKA_RESP_ERR_NO_ERROR));
    }

    public function testNothingToCommitIsSilent(): void
    {
        self::assertNull($this->describe(RD_KAFKA_RESP_ERR__NO_OFFSET));
    }

    public function testRebalanceRejectionIsNarratedAsABenignSkip(): void
    {
        $line = $this->describe(RD_KAFKA_RESP_ERR_ILLEGAL_GENERATION, [
            new TopicPartition('enet.ecommerce.orders', 3, 15),
        ]);

        self::assertNotNull($line);
        self::assertStringContainsString('skipped', $line);
        self::assertStringContainsString('enet.ecommerce.orders[3]@15', $line);
        self::assertStringContainsString('redelivered', $line);
    }

    public function testGenuineFailureIsNarratedAsAnError(): void
    {
        $line = $this->describe(RD_KAFKA_RESP_ERR_OFFSET_METADATA_TOO_LARGE, [
            new TopicPartition('enet.ecommerce.orders', 0, 7),
        ]);

        self::assertNotNull($line);
        self::assertStringContainsString('failed', $line);
        self::assertStringContainsString('enet.ecommerce.orders[0]@7', $line);
    }

    /**
     * @param array<int, TopicPartition> $partitions
     */
    private function describe(int $err, array $partitions = []): ?string
    {
        return (new OffsetCommitCallback())->describe($err, $partitions);
    }
}
