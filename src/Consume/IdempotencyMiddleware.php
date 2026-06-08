<?php

declare(strict_types=1);

namespace Workshop\Consume;

/**
 * The idempotency check, living OUTSIDE the handler. If the event_id is already in
 * the dedup ledger the chain short-circuits — $next is never called, so the handler
 * does not run a second time. Otherwise it runs the handler and then records the id.
 * Run inside TransactionMiddleware, this makes reprocessing a no-op: the visible
 * effect is effectively-once even though Kafka delivery is at-least-once.
 */
final readonly class IdempotencyMiddleware implements Middleware
{
    public function __construct(
        private EventDedup $dedup,
    ) {
    }

    public function handle(ConsumedMessage $message, callable $next): void
    {
        if ($this->dedup->seen($message->eventId)) {
            return; // already processed — skip the handler entirely
        }

        $next($message);
        $this->dedup->record($message);
    }
}
