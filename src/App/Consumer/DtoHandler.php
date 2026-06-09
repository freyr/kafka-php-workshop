<?php

declare(strict_types=1);

namespace Workshop\App\Consumer;

/**
 * What the ConsumerBus dispatches a decoded record's DTO to. The production
 * handler ({@see ProjectionHandler}) folds the DTO into the read-model projection;
 * the Block 4 exercise swaps in {@see FieldPrintHandler}, which just prints the
 * DTO's fields — same consume pipeline, different terminal step. The handler sees
 * only the DTO; envelope concerns (dedup, transactions) stay in bus middleware.
 */
interface DtoHandler
{
    public function handle(object $dto): void;
}
