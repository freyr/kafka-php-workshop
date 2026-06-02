<?php

declare(strict_types=1);

namespace Workshop\Kernel;

/**
 * The three event types the Block 3 demo can produce. Each case knows its
 * topic, schema file, registry subject (TopicNameStrategy: <topic>-value), and
 * fully-qualified event_type string.
 */
enum WorkshopEvent: string
{
    case OrderCreated = 'order-created';
    case PaymentProcessed = 'payment-processed';
    case InventoryReserved = 'inventory-reserved';

    public function topic(): string
    {
        return match ($this) {
            self::OrderCreated => 'enet.ecommerce.orders',
            self::PaymentProcessed => 'enet.ecommerce.payments',
            self::InventoryReserved => 'enet.ecommerce.inventory',
        };
    }

    public function subject(): string
    {
        return $this->topic() . '-value';
    }

    public function eventType(): string
    {
        return match ($this) {
            self::OrderCreated => 'ecommerce.orders.v1.OrderCreated',
            self::PaymentProcessed => 'ecommerce.payments.v1.PaymentProcessed',
            self::InventoryReserved => 'ecommerce.inventory.v1.InventoryReserved',
        };
    }

    public function schemaPath(): string
    {
        $schemas = dirname(__DIR__, 2) . '/schemas';

        return match ($this) {
            self::OrderCreated => $schemas . '/orders/OrderCreated.avsc',
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
