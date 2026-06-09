<?php

declare(strict_types=1);

namespace Workshop\App\Consumer;

use Psr\Container\ContainerInterface;

/**
 * The consumer's command bus. It holds every per-DTO handler (a service locator
 * keyed by DTO class-string, wired by MessageHandlerPass from the tagged
 * #[AsMessageHandler] services) and routes a consumed message to the one handler
 * that claims its DTO — replacing the hand-written instanceof switch a single
 * generic handler used to carry.
 *
 * Delivery semantics stay outside the handlers, as middleware the bus folds around
 * the dispatch: with $idempotent it wraps [transaction, idempotency] (effectively-
 * once); without it the handler runs bare. A DTO no handler claims is a silent
 * no-op — the routing table only hands the bus DTOs this consumer decodes, and not
 * every decoded type projects (audited is decode-and-flow, evolved is print-only).
 *
 * The chain is folded back-to-front so the first middleware is the outermost
 * wrapper (transaction wraps idempotency wraps the handler).
 */
final readonly class MessageBus
{
    public function __construct(
        private ContainerInterface $handlers,
        private TransactionMiddleware $transaction,
        private IdempotencyMiddleware $idempotency,
    ) {
    }

    public function dispatch(ConsumedMessage $message, bool $idempotent): void
    {
        $pipeline = function (ConsumedMessage $m): void {
            $dto = $m->dto;
            $class = $dto::class;
            if (! $this->handlers->has($class)) {
                return; // no handler claims this DTO — decode-and-flow, project nothing
            }
            $handler = $this->handlers->get($class);
            if (is_callable($handler)) {
                $handler($dto);
            }
        };

        $middleware = $idempotent ? [$this->transaction, $this->idempotency] : [];
        foreach (array_reverse($middleware) as $mw) {
            $next = $pipeline;
            $pipeline = static function (ConsumedMessage $m) use ($mw, $next): void {
                $mw->handle($m, $next);
            };
        }

        $pipeline($message);
    }
}
