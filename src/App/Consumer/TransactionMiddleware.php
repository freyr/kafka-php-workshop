<?php

declare(strict_types=1);

namespace Workshop\App\Consumer;

use Doctrine\DBAL\Connection;

/**
 * Wraps the rest of the dispatch in a single DB transaction, so the dedup row and
 * the projection write commit together or not at all — the "common transaction" the
 * idempotent strategy depends on. It is the outermost middleware, so the dedup
 * check, the handler, and the dedup insert all run inside the same transaction.
 */
final readonly class TransactionMiddleware implements Middleware
{
    public function __construct(
        private Connection $connection,
    ) {
    }

    public function handle(ConsumedMessage $message, callable $next): void
    {
        $this->connection->transactional(static function () use ($message, $next): void {
            $next($message);
        });
    }
}
