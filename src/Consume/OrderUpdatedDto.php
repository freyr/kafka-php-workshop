<?php

declare(strict_types=1);

namespace Workshop\Consume;

final readonly class OrderUpdatedDto
{
    public function __construct(
        public string $orderId,
        public string $status,
        public ?string $previousStatus = null,
    ) {
    }
}
