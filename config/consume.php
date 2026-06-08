<?php

declare(strict_types=1);

use Workshop\Consume\OrderCancelledDto;
use Workshop\Consume\OrderCreatedDto;
use Workshop\Consume\OrderUpdatedDto;

// Consume-side routing: message name => read-model DTO. Only the names this
// service consumes are listed; payment/inventory are not dispatched here.
return [
    'order-created' => OrderCreatedDto::class,
    'order-updated' => OrderUpdatedDto::class,
    'order-cancelled' => OrderCancelledDto::class,
];
