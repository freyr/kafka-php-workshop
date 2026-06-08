<?php

declare(strict_types=1);

namespace Workshop\Produce;

#[MessageName('payment.processed')]
final class PaymentProcessed extends Message
{
    public static function create(string $orderId, string $status = 'SUCCEEDED'): self
    {
        $failed = 'SUCCEEDED' !== $status;

        return new self($orderId, [
            'payment_id' => self::generateId('pay'),
            'order_id' => $orderId,
            'status' => $status,
            'payment_method' => 'BLIK',
            'amount' => self::money(8606),
            'gateway_transaction_id' => $failed ? null : self::generateId('txn'),
            'failure_reason' => $failed ? 'Insufficient funds' : null,
            'failure_code' => $failed ? 'INSUFFICIENT_FUNDS' : null,
            'processed_at' => self::nowMillis(),
        ]);
    }
}
