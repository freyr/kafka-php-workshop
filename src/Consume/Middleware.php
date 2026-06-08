<?php

declare(strict_types=1);

namespace Workshop\Consume;

/**
 * One link in the ConsumerBus chain. A middleware wraps the dispatch of a
 * ConsumedMessage: it may run work before and after $next, or short-circuit by not
 * calling $next at all (idempotency skips an already-seen event). Middleware sees
 * the whole envelope (it needs the event_id); the handler at the end of the chain
 * sees only the DTO.
 */
interface Middleware
{
    /**
     * @param callable(ConsumedMessage): void $next the rest of the chain
     */
    public function handle(ConsumedMessage $message, callable $next): void;
}
