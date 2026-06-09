<?php

declare(strict_types=1);

namespace Workshop\App\Consumer;

/**
 * The read model for order.audited — the minimal event behind the consumer-group
 * offsets demo. It carries only what the audit log needs; no #[AsMessageHandler]
 * claims it, so the MessageBus routes it nowhere — a deliberate no-op (handled, not
 * skipped): the point of routing it is to show a record decode and flow through the
 * bus, not to project anything.
 */
final readonly class OrderAuditedDto implements OrderEvent
{
    public function __construct(
        public string $orderId,
        public string $action,
    ) {
    }
}
