<?php

declare(strict_types=1);

namespace Workshop\Tests\Produce;

use Workshop\Produce\Message;
use Workshop\Produce\MessageName;

#[MessageName('order-created')]
final class MessageTestDouble extends Message
{
    public function __construct(
        private readonly string $id,
    ) {
        parent::__construct();
    }

    public function partitionKey(): string
    {
        return $this->id;
    }

    public function toPayload(): array
    {
        return [
            'order_id' => $this->id,
            'status' => 'NEW',
        ];
    }
}
