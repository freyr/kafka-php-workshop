<?php

declare(strict_types=1);

namespace Workshop\Consume;

final readonly class OrderCancelledDto
{
    public function __construct(
        public string $orderId,
        public string $reason,
    ) {
    }
}
