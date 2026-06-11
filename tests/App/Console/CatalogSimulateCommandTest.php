<?php

declare(strict_types=1);

namespace Workshop\Tests\App\Console;

use Doctrine\DBAL\Connection;
use FlixTech\SchemaRegistryApi\Exception\SubjectNotFoundException;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Tester\CommandTester;
use Workshop\App\Catalog\CatalogChangePlacer;
use Workshop\App\Catalog\CatalogStateChangeRepository;
use Workshop\App\Console\CatalogSimulateCommand;
use Workshop\App\Producer\Message;
use Workshop\App\Producer\MessageNameResolver;
use Workshop\Kafka\Serde\MessageSerializer;

/**
 * The placer is real and runs against one mocked Connection whose transactional()
 * executes the callback, so the command's happy path exercises the full placer
 * flow with no database (mirrors OutboxPlaceCommandTest).
 */
final class CatalogSimulateCommandTest extends TestCase
{
    public function testCountAndNewMustTogetherBeAtLeastOne(): void
    {
        $tester = $this->tester(self::createStub(Connection::class));

        $tester->execute([
            '--count' => '0',
            '--new' => '0',
        ]);

        self::assertSame(Command::INVALID, $tester->getStatusCode());
        self::assertStringContainsString('--count and --new must be >= 0', $tester->getDisplay());
    }

    public function testNegativeCountIsRejected(): void
    {
        $tester = $this->tester(self::createStub(Connection::class));

        $tester->execute([
            '--count' => '-1',
        ]);

        self::assertSame(Command::INVALID, $tester->getStatusCode());
        self::assertStringContainsString('--count and --new must be >= 0', $tester->getDisplay());
    }

    public function testMissingSubjectFailsWithTheRegisterHint(): void
    {
        $serializer = new class implements MessageSerializer {
            public function encode(Message $payload): string
            {
                throw new SubjectNotFoundException('Subject not found');
            }

            public function decode(string $bytes, ?\AvroSchema $readerSchema = null): mixed
            {
                return null;
            }
        };

        $tester = $this->tester(self::createStub(Connection::class), $serializer);

        $tester->execute([
            '--count' => '1',
            '--interval' => '0',
        ]);

        self::assertSame(Command::FAILURE, $tester->getStatusCode());
        self::assertStringContainsString('No schema registered for catalog.projection_change', $tester->getDisplay());
        self::assertStringContainsString('kafka:schema:register catalog.projection_change', $tester->getDisplay());
    }

    public function testNewProductsArePlacedFirstThenPoolChanges(): void
    {
        $connection = $this->connection();

        $tester = $this->tester($connection);

        $tester->execute([
            '--count' => '2',
            '--new' => '1',
            '--interval' => '0',
        ]);

        self::assertSame(Command::SUCCESS, $tester->getStatusCode(), $tester->getDisplay());

        $display = $tester->getDisplay();
        self::assertSame(3, substr_count($display, 'placed catalog.projection_change'));

        // The minted SKUs lead the run — the first placed line is the new product.
        $lines = array_values(array_filter(explode("\n", $display), static fn (string $line): bool => str_contains($line, 'placed catalog.projection_change')));
        self::assertStringContainsString('(new product)', $lines[0]);
        self::assertStringContainsString('SKU-NEW-', $lines[0]);

        self::assertStringContainsString('3 full-state event(s) appended', $display);
    }

    private function connection(): Connection
    {
        $connection = self::createStub(Connection::class);
        $connection->method('transactional')
            ->willReturnCallback(static fn (\Closure $func): mixed => $func());
        $connection->method('executeStatement')->willReturn(1);

        return $connection;
    }

    private function tester(Connection $connection, ?MessageSerializer $serializer = null): CommandTester
    {
        $serializer ??= new class implements MessageSerializer {
            public function encode(Message $payload): string
            {
                return "\x00FRAMED-AVRO";
            }

            public function decode(string $bytes, ?\AvroSchema $readerSchema = null): mixed
            {
                return null;
            }
        };

        $placer = new CatalogChangePlacer(
            $connection,
            new CatalogStateChangeRepository($connection),
            new MessageNameResolver(),
            $serializer,
        );

        return new CommandTester(new CatalogSimulateCommand($placer));
    }
}
