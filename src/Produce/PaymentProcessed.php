<?php

declare(strict_types=1);

namespace Workshop\Produce;

#[MessageName('payment-processed')]
final class PaymentProcessed extends Message
{
    private readonly string $paymentId;
    private readonly bool $failed;
    private readonly ?string $gatewayTransactionId;

    public function __construct(
        private readonly string $orderId,
        private readonly string $status = 'SUCCEEDED',
    ) {
        parent::__construct();
        $this->paymentId = self::generateId('pay');
        $this->failed = 'SUCCEEDED' !== $status;
        $this->gatewayTransactionId = $this->failed ? null : self::generateId('txn');
    }

    public function partitionKey(): string
    {
        return $this->orderId;
    }

    public function toPayload(): array
    {
        return [
            'payment_id' => $this->paymentId,
            'order_id' => $this->orderId,
            'status' => $this->status,
            'payment_method' => 'BLIK',
            'amount' => self::money(8606),
            'gateway_transaction_id' => $this->gatewayTransactionId,
            'failure_reason' => $this->failed ? 'Insufficient funds' : null,
            'failure_code' => $this->failed ? 'INSUFFICIENT_FUNDS' : null,
            'processed_at' => self::nowMillis(),
        ];
    }
}
