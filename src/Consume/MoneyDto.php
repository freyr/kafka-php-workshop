<?php

declare(strict_types=1);

namespace Workshop\Consume;

final readonly class MoneyDto
{
    public function __construct(
        public int $amountCents,
    ) {
    }
}
