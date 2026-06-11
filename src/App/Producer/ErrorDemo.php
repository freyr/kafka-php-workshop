<?php

declare(strict_types=1);

namespace Workshop\App\Producer;

/**
 * The Block 7 error-handling demo event. Deliberately minimal — the demo is about
 * what the consumer does when handling fails, not about the payload. Placed only
 * through the outbox (`outbox:place --message-name error.demo`) onto its dedicated
 * topic family; excluded from every random-pick catalog so the order demos can
 * never receive one.
 */
#[MessageName('error.demo')]
final class ErrorDemo extends Message
{
    public static function create(string $id, int $seq, string $note = 'error-handling demo'): self
    {
        return new self($id, [
            'id' => $id,
            'seq' => $seq,
            'note' => $note,
        ]);
    }
}
