<?php

declare(strict_types=1);

namespace Workshop\App\Consumer;

final readonly class CustomerRefDto
{
    public function __construct(
        public string $displayName,
    ) {
    }
}
