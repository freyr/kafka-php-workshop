<?php

declare(strict_types=1);

namespace Workshop\Tests\App\Producer;

use Workshop\App\Producer\Message;
use Workshop\App\Producer\MessageName;

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
