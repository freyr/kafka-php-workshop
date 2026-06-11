<?php

declare(strict_types=1);

namespace Workshop\Tests\App\Consumer;

use Doctrine\DBAL\Connection;
use PHPUnit\Framework\TestCase;
use Workshop\App\Consumer\FailureModeSwitch;

final class FailureModeSwitchTest extends TestCase
{
    public function testEnabledReadsTheFlagRow(): void
    {
        $connection = $this->createMock(Connection::class);
        $connection->method('fetchOne')
            ->with(self::stringContains('runtime_flags'), ['transient-failure'])
            ->willReturn('1');

        self::assertTrue(new FailureModeSwitch($connection)->enabled());
    }

    public function testAMissingRowMeansDisabled(): void
    {
        $connection = $this->createMock(Connection::class);
        $connection->method('fetchOne')->willReturn(false);

        self::assertFalse(new FailureModeSwitch($connection)->enabled());
    }

    public function testEnableUpsertsTheFlagOn(): void
    {
        $connection = $this->createMock(Connection::class);
        $connection->expects(self::once())
            ->method('executeStatement')
            ->with(self::stringContains('runtime_flags'), [
                'name' => 'transient-failure',
                'enabled' => 1,
            ]);

        new FailureModeSwitch($connection)->enable();
    }

    public function testDisableUpsertsTheFlagOff(): void
    {
        $connection = $this->createMock(Connection::class);
        $connection->expects(self::once())
            ->method('executeStatement')
            ->with(self::stringContains('runtime_flags'), [
                'name' => 'transient-failure',
                'enabled' => 0,
            ]);

        new FailureModeSwitch($connection)->disable();
    }
}
