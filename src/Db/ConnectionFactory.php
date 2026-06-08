<?php

declare(strict_types=1);

namespace Workshop\Db;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Tools\DsnParser;

/**
 * Turns the DATABASE_URL string (wired as the %database.url% parameter) into a live
 * DBAL Connection — the single seam between config-as-data and the consumer's store,
 * mirroring how ConfBuilder is the only place a Kafka client is constructed.
 *
 * DBAL 4 dropped the implicit `url` connection parameter, so the URL is parsed
 * explicitly with DsnParser. The scheme map points the `mysql://` DSN at the
 * pdo_mysql driver (the extension baked into the php image), so a plain
 * mysql://user:pass@host/db URL resolves without naming a driver in the URL.
 */
final readonly class ConnectionFactory
{
    public function __construct(
        private string $url,
    ) {
    }

    public function create(): Connection
    {
        $params = new DsnParser([
            'mysql' => 'pdo_mysql',
        ])->parse($this->url);

        return DriverManager::getConnection($params);
    }
}
