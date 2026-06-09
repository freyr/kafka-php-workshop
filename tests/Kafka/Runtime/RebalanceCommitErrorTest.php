<?php

declare(strict_types=1);

namespace Workshop\Tests\Kafka\Runtime;

use PHPUnit\Framework\TestCase;
use Workshop\Kafka\Runtime\RebalanceCommitError;

final class RebalanceCommitErrorTest extends TestCase
{
    public function testTheThreeRebalanceClassCodesMatch(): void
    {
        self::assertTrue(RebalanceCommitError::matches(RD_KAFKA_RESP_ERR_ILLEGAL_GENERATION));
        self::assertTrue(RebalanceCommitError::matches(RD_KAFKA_RESP_ERR_REBALANCE_IN_PROGRESS));
        self::assertTrue(RebalanceCommitError::matches(RD_KAFKA_RESP_ERR_UNKNOWN_MEMBER_ID));
    }

    public function testSuccessAndGenuineFaultsDoNotMatch(): void
    {
        self::assertFalse(RebalanceCommitError::matches(RD_KAFKA_RESP_ERR_NO_ERROR));
        self::assertFalse(RebalanceCommitError::matches(RD_KAFKA_RESP_ERR_OFFSET_METADATA_TOO_LARGE));
        self::assertFalse(RebalanceCommitError::matches(RD_KAFKA_RESP_ERR__FAIL));
    }
}
