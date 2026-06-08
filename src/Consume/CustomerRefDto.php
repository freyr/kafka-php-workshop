<?php

declare(strict_types=1);

namespace Workshop\Consume;

final readonly class CustomerRefDto
{
    public function __construct(
        public string $displayName,
    ) {
    }
}
