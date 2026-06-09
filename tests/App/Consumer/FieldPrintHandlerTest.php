<?php

declare(strict_types=1);

namespace Workshop\Tests\App\Consumer;

use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Output\BufferedOutput;
use Workshop\App\Consumer\ConsoleWriter;
use Workshop\App\Consumer\FieldPrintHandler;
use Workshop\App\Consumer\OrderEvolvedDto;

final class FieldPrintHandlerTest extends TestCase
{
    public function testPrintsTheDtoPublicFields(): void
    {
        $output = new BufferedOutput();
        $console = new ConsoleWriter();
        $console->bind($output);

        new FieldPrintHandler($console)(new OrderEvolvedDto('ord-9', 'Jane', 1500));

        $display = $output->fetch();
        self::assertStringContainsString('orderId', $display);
        self::assertStringContainsString('ord-9', $display);
        self::assertStringContainsString('customerName', $display);
        self::assertStringContainsString('Jane', $display);
        self::assertStringContainsString('amountCents', $display);
        self::assertStringContainsString('1500', $display);
    }
}
