<?php

declare(strict_types=1);

namespace Workshop\Tests\Framework\Db;

use Doctrine\DBAL\Connection;
use PHPUnit\Framework\TestCase;
use Workshop\Framework\Db\OutboxSchemaInstaller;

final class OutboxSchemaInstallerTest extends TestCase
{
    public function testInstallProvisionsABinaryPayloadColumn(): void
    {
        $connection = $this->createMock(Connection::class);
        $connection->expects(self::once())
            ->method('executeStatement')
            ->with(self::logicalAnd(
                self::stringContains('CREATE TABLE IF NOT EXISTS outbox'),
                self::stringContains('payload        MEDIUMBLOB'),
            ));

        new OutboxSchemaInstaller($connection)->install();
    }

    public function testPayloadColumnTypeNormalizesWhatMysqlReports(): void
    {
        // A pre-AVRO provisioning left a JSON column behind — the fingerprint
        // must surface it (lowercased) so setup can demand --fresh.
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
