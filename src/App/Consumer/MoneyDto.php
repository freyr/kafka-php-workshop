<?php

declare(strict_types=1);

namespace Workshop\App\Consumer;

final readonly class MoneyDto
{
    public function __construct(
        public int $amountCents,
    ) {
    }
}
