<?php

declare(strict_types=1);

namespace Workshop\Consume;

final readonly class OrderTotalsDto
{
    public function __construct(
        public MoneyDto $total,
    ) {
    }
}
