<?php

declare(strict_types=1);

namespace Workshop\Kernel;

/**
 * The event types the Block 3 demo can produce. Each case knows its topic,
 * schema file, registry subject (RecordNameStrategy: the fully-qualified record
 * name, record component in lower_snake_case), and fully-qualified event_type
 * string.
 *
 * OrderCreated, OrderUpdated, and OrderCancelled all live on the SAME
 * enet.ecommerce.orders topic — the workshop's multiple-event-types-per-topic
 * case. Each still registers as its own subject, so its compatibility lineage is
 * independent of its topic-mates. A consumer of that topic dispatches by
 * event_type (see events:dispatch).
 */
enum WorkshopEvent: string
{
    case OrderCreated = 'order-created';
    case OrderUpdated = 'order-updated';
    case OrderCancelled = 'order-cancelled';
    case PaymentProcessed = 'payment-processed';
    case InventoryReserved = 'inventory-reserved';

    public function topic(): string
    {
        return match ($this) {
            self::OrderCreated, self::OrderUpdated, self::OrderCancelled => 'enet.ecommerce.orders',
            self::PaymentProcessed => 'enet.ecommerce.payments',
            self::InventoryReserved => 'enet.ecommerce.inventory',
        };
    }

    public function subject(): string
    {
        // RecordNameStrategy: the subject is the schema's fully-qualified record
        // name (namespace + record), so each event type carries its own
        // compatibility lineage independent of the topic it shares. The record
        // component is lower_snake_case by our naming convention.
        return match ($this) {
            self::OrderCreated => 'com.ecommerce.orders.v1.order_created',
            self::OrderUpdated => 'com.ecommerce.orders.v1.order_updated',
            self::OrderCancelled => 'com.ecommerce.orders.v1.order_cancelled',
            self::PaymentProcessed => 'com.ecommerce.payments.v1.payment_processed',
            self::InventoryReserved => 'com.ecommerce.inventory.v1.inventory_reserved',
        };
    }

    public function eventType(): string
    {
        return match ($this) {
            self::OrderCreated => 'ecommerce.orders.v1.OrderCreated',
            self::OrderUpdated => 'ecommerce.orders.v1.OrderUpdated',
            self::OrderCancelled => 'ecommerce.orders.v1.OrderCancelled',
            self::PaymentProcessed => 'ecommerce.payments.v1.PaymentProcessed',
            self::InventoryReserved => 'ecommerce.inventory.v1.InventoryReserved',
        };
    }

    public function schemaPath(): string
    {
        $schemas = dirname(__DIR__, 2) . '/schemas';

        return match ($this) {
            self::OrderCreated => $schemas . '/orders/OrderCreated.avsc',
            self::OrderUpdated => $schemas . '/orders/OrderUpdated.avsc',
            self::OrderCancelled => $schemas . '/orders/OrderCancelled.avsc',
            self::PaymentProcessed => $schemas . '/payments/PaymentProcessed.avsc',
            self::InventoryReserved => $schemas . '/inventory/InventoryReserved.avsc',
        };
    }

    public function schemaJson(): string
    {
        $json = file_get_contents($this->schemaPath());
        if (false === $json) {
            throw new \RuntimeException("Unable to read schema file: {$this->schemaPath()}");
        }

        return $json;
    }
}
