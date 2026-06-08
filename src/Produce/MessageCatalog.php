<?php

declare(strict_types=1);

namespace Workshop\Produce;

/**
 * The set of AVRO messages the `produce` command can emit, and how to build a
 * representative instance of each from an order id. This is the produce-side
 * counterpart to the routing table: MessageRouting knows where a name goes,
 * MessageCatalog knows how to construct it. Keeping the name => class mapping
 * here lets `produce` pick a random message (or one pinned --message-name)
 * without each command re-stating the match.
 *
 * Every message keys on the order id (its partition key), so handing the whole
 * catalog one id from a small pool lets several event types land on the same
 * aggregate — the same key, the same partition, ordered — which is what makes a
 * randomized stream still demonstrate per-order ordering.
 */
final class MessageCatalog
{
    /**
     * The buildable message names, in catalog order. Mirrors the AVRO routes in
     * config/producers.yaml — `produce` emits AVRO only.
     *
     * @return list<string>
     */
    public function names(): array
    {
        return [
            'order.created',
            'order.updated',
            'order.cancelled',
            'payment.processed',
            'inventory.reserved',
        ];
    }

    public function has(string $name): bool
    {
        return in_array($name, $this->names(), true);
    }

    /**
     * Build a representative message of the given name keyed by $orderId. Optional
     * per-type fields (status, reason) use their defaults — `produce` emits a
     * stream of representative events, not hand-tuned individual ones.
     */
    public function build(string $name, string $orderId): Message
    {
        return match ($name) {
            'order.created' => OrderCreated::create($orderId),
            'order.updated' => OrderUpdated::create($orderId),
            'order.cancelled' => OrderCancelled::create($orderId),
            'payment.processed' => PaymentProcessed::create($orderId),
            'inventory.reserved' => InventoryReserved::create($orderId),
            default => throw new \InvalidArgumentException(sprintf("No AVRO message named '%s'. Available: %s", $name, implode(', ', $this->names()))),
        };
    }
}
