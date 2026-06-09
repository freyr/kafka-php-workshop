<?php

declare(strict_types=1);

namespace Workshop\App\Consumer;

final readonly class OrderUpdatedDto
{
    public function __construct(
        public string $orderId,
        public string $status,
        public ?string $previousStatus = null,
    ) {
    }
}
