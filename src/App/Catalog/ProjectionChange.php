<?php

declare(strict_types=1);

namespace Workshop\App\Catalog;

use Workshop\App\Producer\Message;
use Workshop\App\Producer\MessageName;

/**
 * Block 9 state-transfer event: the FULL current state of one product, published
 * on every change. No deltas — a consumer that upserts on sku always converges
 * on the catalog's truth, and THAT is what makes the zero-code JDBC-sink
 * projection possible. Keyed by sku, so all changes to one product stay in
 * partition order. Prices are minor units (grosze) per the Money-as-cents
 * convention.
 */
#[MessageName('catalog.projection_change')]
final class ProjectionChange extends Message
{
    public static function create(string $sku, string $name, int $priceCents, int $marginCents): self
    {
        return new self($sku, [
            'sku' => $sku,
            'name' => $name,
            'price' => $priceCents,
            'margin' => $marginCents,
        ]);
    }
}
