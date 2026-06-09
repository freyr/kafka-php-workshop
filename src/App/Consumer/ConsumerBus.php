<?php

declare(strict_types=1);

namespace Workshop\App\Consumer;

/**
 * A minimal command bus: it dispatches a ConsumedMessage through an ordered list of
 * middleware and finally to the handler, which receives only the DTO. The middleware
 * list is the seam the consume command varies per --commit strategy — empty for
 * per-message/auto, [Transaction, Idempotency] for the idempotent strategy — so the
 * delivery semantics live entirely outside the handler.
 *
 * The chain is folded back-to-front so the first middleware in the list is the
 * outermost wrapper (Transaction wraps Idempotency wraps the handler).
 */
final readonly class ConsumerBus
{
    /**
     * @param list<Middleware> $middleware outermost first
     */
    public function __construct(
        private DtoHandler $handler,
        private array $middleware = [],
    ) {
    }

    public function dispatch(ConsumedMessage $message): void
    {
        $pipeline = function (ConsumedMessage $m): void {
            $this->handler->handle($m->dto);
        };

        foreach (array_reverse($this->middleware) as $middleware) {
            $next = $pipeline;
            $pipeline = static function (ConsumedMessage $m) use ($middleware, $next): void {
                $middleware->handle($m, $next);
            };
        }

        $pipeline($message);
    }
}
