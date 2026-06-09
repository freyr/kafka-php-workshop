<?php

declare(strict_types=1);

namespace Workshop\App\Consumer;

final readonly class OrderCancelledDto implements OrderEvent
{
    public function __construct(
        public string $orderId,
        public string $reason,
    ) {
    }
}
