<?php

declare(strict_types=1);

namespace Workshop\Tests\Produce;

use Workshop\Produce\Message;
use Workshop\Produce\MessageName;

#[MessageName('order.created')]
final class MessageTestDouble extends Message
{
    public static function create(string $id): self
    {
        return new self($id, [
            'order_id' => $id,
            'status' => 'NEW',
        ]);
    }
}
