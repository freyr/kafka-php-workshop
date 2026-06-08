<?php

declare(strict_types=1);

namespace Workshop\Consume;

/**
 * Read model for order-created — only the fields the dispatcher renders. The
 * Serializer ignores payload keys not declared here (items, shipping_address,
 * placed_at, notes), which is the point of a consumer-owned read model.
 */
final readonly class OrderCreatedDto
{
    public function __construct(
        public string $orderId,
        public CustomerRefDto $customer,
        public OrderTotalsDto $totals,
    ) {
    }
}
