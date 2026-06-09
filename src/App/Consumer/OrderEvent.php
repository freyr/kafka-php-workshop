<?php

declare(strict_types=1);

namespace Workshop\App\Consumer;

/**
 * The shared shape of every consumed order read-model DTO: an order id. It lets the
 * run log describe any consumed event by its order without a per-type switch — the
 * id is the one field they all carry. The promoted `public string $orderId` on each
 * DTO already satisfies this get-only interface property (PHP 8.4), so implementing
 * it costs the DTOs nothing but the declaration.
 */
interface OrderEvent
{
    public string $orderId { get; }
}
