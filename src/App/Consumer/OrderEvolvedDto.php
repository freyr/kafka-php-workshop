<?php

declare(strict_types=1);

namespace Workshop\App\Consumer;

/**
 * Read model for the Block 4 schema-evolution playground event. Starts flat —
 * the three fields the baseline schema carries. During the exercise you add a
 * property here (e.g. `public string $loyaltyTier`) to make the consumer read the
 * field you evolved into the schema, and watch how writer vs. latest reader
 * schemas change whether old records can still hydrate this DTO.
 */
final readonly class OrderEvolvedDto implements OrderEvent
{
    public function __construct(
        public string $orderId,
        public string $customerName,
        public int $amountCents,
    ) {
    }
}
