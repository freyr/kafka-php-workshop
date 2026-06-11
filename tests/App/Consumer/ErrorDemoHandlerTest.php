<?php

declare(strict_types=1);

namespace Workshop\Tests\App\Consumer;

use Doctrine\DBAL\Connection;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Output\BufferedOutput;
use Workshop\App\Consumer\ConsoleWriter;
use Workshop\App\Consumer\ErrorDemoDto;
use Workshop\App\Consumer\ErrorDemoHandler;
use Workshop\App\Consumer\FailureModeSwitch;
use Workshop\Kafka\Runtime\TransientException;

final class ErrorDemoHandlerTest extends TestCase
{
    public function testAppliesAndNarratesWhileFailureModeIsOff(): void
    {
        $output = new BufferedOutput();
        $console = new ConsoleWriter();
        $console->bind($output);

        $this->handler(failureMode: false, console: $console)(new ErrorDemoDto('err-1', 7, 'demo'));

        self::assertStringContainsString('applied error.demo seq=7 id=err-1', strip_tags($output->fetch()));
    }

    public function testThrowsTransientWhileFailureModeIsOn(): void
    {
        $this->expectException(TransientException::class);
        $this->expectExceptionMessageMatches('/simulated transient outage/');

        $this->handler(failureMode: true, console: new ConsoleWriter())(new ErrorDemoDto('err-1', 7, 'demo'));
    }

    private function handler(bool $failureMode, ConsoleWriter $console): ErrorDemoHandler
    {
        $connection = $this->createMock(Connection::class);
        $connection->method('fetchOne')->willReturn($failureMode ? 1 : false);

        return new ErrorDemoHandler(new FailureModeSwitch($connection), $console);
    }
}
