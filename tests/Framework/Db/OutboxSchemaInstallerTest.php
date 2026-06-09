<?php

declare(strict_types=1);

namespace Workshop\Tests\Framework\Db;

use Doctrine\DBAL\Connection;
use PHPUnit\Framework\TestCase;
use Workshop\App\Outbox\PayloadFormat;
use Workshop\Framework\Db\OutboxSchemaInstaller;

final class OutboxSchemaInstallerTest extends TestCase
{
    public function testJsonFormatProvisionsAJsonPayloadColumn(): void
    {
        $connection = $this->createMock(Connection::class);
        $connection->expects(self::once())
            ->method('executeStatement')
            ->with(self::logicalAnd(
                self::stringContains('CREATE TABLE IF NOT EXISTS outbox'),
                self::stringContains('payload        JSON'),
            ));

        new OutboxSchemaInstaller($connection)->install();
    }

    public function testAvroFormatProvisionsABinaryPayloadColumn(): void
    {
        $connection = $this->createMock(Connection::class);
        $connection->expects(self::once())
            ->method('executeStatement')
            ->with(self::stringContains('payload        MEDIUMBLOB'));

        new OutboxSchemaInstaller($connection)->install(PayloadFormat::Avro);
    }

    public function testPayloadColumnTypeNarrowsTheLookup(): void
    {
        $connection = self::createStub(Connection::class);
        $connection->method('fetchOne')->willReturn('JSON');

        self::assertSame('json', new OutboxSchemaInstaller($connection)->payloadColumnType());
    }

    public function testPayloadColumnTypeIsNullWhenTheTableIsMissing(): void
    {
        $connection = self::createStub(Connection::class);
        $connection->method('fetchOne')->willReturn(false);

        self::assertNull(new OutboxSchemaInstaller($connection)->payloadColumnType());
    }
}
