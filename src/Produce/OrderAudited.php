<?php

declare(strict_types=1);

namespace Workshop\Produce;

/**
 * A deliberately tiny event for the consumer-group offsets demo. It is routed to
 * the single-partition enet.ecommerce.audit topic, so every record lands on the
 * one partition and each consumer group's committed offset advances 1:1 with the
 * stream — the "simple offset math" that makes per-group offsets easy to read in
 * kafka-ui.
 */
#[MessageName('order.audited')]
final class OrderAudited extends Message
{
    public static function create(string $orderId): self
    {
        return new self($orderId, [
            'order_id' => $orderId,
            'action' => 'observed',
        ]);
    }
}
