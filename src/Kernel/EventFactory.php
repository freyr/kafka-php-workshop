<?php

declare(strict_types=1);

namespace Workshop\Kernel;

use Symfony\Component\Uid\Uuid;

/**
 * Builds sample enveloped event payloads (metadata + business payload) for the
 * Block 3 demo. The shapes match the AVRO schemas under schemas/. Ids are UUID
 * v7 (time-ordered) via symfony/uid; timestamps are UTC epoch millis.
 */
final class EventFactory
{
    private const string SOURCE_SERVICE = 'workshop-cli';
    private const string DEFAULT_TENANT = 'tenant-acme-corp';

    /**
     * @param array<string, string> $opts order_id, correlation_id, causation_id, tenant_id, status
     *
     * @return array<string, mixed>
     */
    public function build(WorkshopEvent $type, array $opts = []): array
    {
        return match ($type) {
            WorkshopEvent::OrderCreated => $this->orderCreated($opts),
            WorkshopEvent::PaymentProcessed => $this->paymentProcessed($opts),
            WorkshopEvent::InventoryReserved => $this->inventoryReserved($opts),
        };
    }

    /**
     * @param array<string, string> $opts
     *
     * @return array<string, mixed>
     */
    private function orderCreated(array $opts): array
    {
        $orderId = $opts['order_id'] ?? $this->generateId('ord');

        return [
            'metadata' => $this->metadata(WorkshopEvent::OrderCreated, $orderId, $opts),
            'order_id' => $orderId,
            'customer' => [
                'customer_id' => 'cust-9876',
                'email' => 'jan@example.com',
                'display_name' => 'Jan Kowalski',
            ],
            'items' => [
                [
                    'product_id' => 'prod-555',
                    'sku' => 'TSHIRT-BLU-L',
                    'product_name' => 'Blue T-Shirt Large',
                    'quantity' => 2,
                    'unit_price' => $this->money(2999),
                    'line_total' => $this->money(5998),
                ],
            ],
            'shipping_address' => [
                'street' => 'ul. Marszalkowska 1',
                'city' => 'Warszawa',
                'postal_code' => '00-001',
                'country' => 'PL',
                'state' => null,
            ],
            'totals' => [
                'subtotal' => $this->money(5998),
                'shipping_cost' => $this->money(999),
                'tax' => $this->money(1609),
                'total' => $this->money(8606),
            ],
            'placed_at' => $this->nowMillis(),
            'notes' => null,
        ];
    }

    /**
     * @param array<string, string> $opts
     *
     * @return array<string, mixed>
     */
    private function paymentProcessed(array $opts): array
    {
        // Payments are keyed by orderId (Block 2) to correlate with the order.
        $orderId = $opts['order_id'] ?? $this->generateId('ord');
        $status = $opts['status'] ?? 'SUCCEEDED';
        $failed = 'SUCCEEDED' !== $status;

        return [
            'metadata' => $this->metadata(WorkshopEvent::PaymentProcessed, $orderId, $opts),
            'payment_id' => $this->generateId('pay'),
            'order_id' => $orderId,
            'status' => $status,
            'payment_method' => 'BLIK',
            'amount' => $this->money(8606),
            'gateway_transaction_id' => $failed ? null : $this->generateId('txn'),
            'failure_reason' => $failed ? 'Insufficient funds' : null,
            'failure_code' => $failed ? 'INSUFFICIENT_FUNDS' : null,
            'processed_at' => $this->nowMillis(),
        ];
    }

    /**
     * @param array<string, string> $opts
     *
     * @return array<string, mixed>
     */
    private function inventoryReserved(array $opts): array
    {
        // Keyed by orderId here so the whole order flow shares one correlation
        // chain; a production inventory topic would more often key by productId.
        $orderId = $opts['order_id'] ?? $this->generateId('ord');

        return [
            'metadata' => $this->metadata(WorkshopEvent::InventoryReserved, $orderId, $opts),
            'reservation_id' => $this->generateId('rsv'),
            'order_id' => $orderId,
            'reserved_items' => [
                [
                    'product_id' => 'prod-555',
                    'sku' => 'TSHIRT-BLU-L',
                    'quantity' => 2,
                    'warehouse_id' => 'wh-waw-01',
                    'warehouse_location' => 'A-12-3',
                ],
            ],
            'reserved_at' => $this->nowMillis(),
            'expires_at' => $this->nowMillis() + 900_000,
        ];
    }

    /**
     * @param array<string, string> $opts
     *
     * @return array<string, mixed>
     */
    private function metadata(WorkshopEvent $type, string $aggregateId, array $opts): array
    {
        return [
            'event_id' => $this->uuid(),
            'event_type' => $type->eventType(),
            'timestamp' => $this->nowMillis(),
            'source_service' => self::SOURCE_SERVICE,
            'correlation_id' => $opts['correlation_id'] ?? $this->uuid(),
            'causation_id' => $opts['causation_id'] ?? null,
            'tenant_id' => $opts['tenant_id'] ?? self::DEFAULT_TENANT,
            'aggregate_id' => $aggregateId,
            'schema_version' => 1,
        ];
    }

    /**
     * @return array{amount_cents: int, currency: string}
     */
    private function money(int $cents): array
    {
        return [
            'amount_cents' => $cents,
            'currency' => 'PLN',
        ];
    }

    private function uuid(): string
    {
        return Uuid::v7()->toRfc4122();
    }

    private function generateId(string $prefix): string
    {
        return $prefix . '-' . substr(Uuid::v4()->toRfc4122(), 0, 8);
    }

    private function nowMillis(): int
    {
        return (int) floor(microtime(true) * 1000);
    }
}
