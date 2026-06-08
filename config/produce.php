<?php

declare(strict_types=1);

// Produce-side routing: message name => wire identity. Subjects use
// RecordNameStrategy (the schema's fully-qualified record name, lower_snake
// component) and carry NO version marker — a breaking change mints a brand-new
// subject, never a vN suffix. order-* share one topic (multiple event types per
// topic); each registers its own subject.
$schemas = dirname(__DIR__) . '/schemas';

return [
    'order-created' => [
        'topic' => 'enet.ecommerce.orders',
        'subject' => 'com.ecommerce.orders.order_created',
        'schema' => $schemas . '/orders/OrderCreated.avsc',
    ],
    'order-updated' => [
        'topic' => 'enet.ecommerce.orders',
        'subject' => 'com.ecommerce.orders.order_updated',
        'schema' => $schemas . '/orders/OrderUpdated.avsc',
    ],
    'order-cancelled' => [
        'topic' => 'enet.ecommerce.orders',
        'subject' => 'com.ecommerce.orders.order_cancelled',
        'schema' => $schemas . '/orders/OrderCancelled.avsc',
    ],
    'payment-processed' => [
        'topic' => 'enet.ecommerce.payments',
        'subject' => 'com.ecommerce.payments.payment_processed',
        'schema' => $schemas . '/payments/PaymentProcessed.avsc',
    ],
    'inventory-reserved' => [
        'topic' => 'enet.ecommerce.inventory',
        'subject' => 'com.ecommerce.inventory.inventory_reserved',
        'schema' => $schemas . '/inventory/InventoryReserved.avsc',
    ],
];
