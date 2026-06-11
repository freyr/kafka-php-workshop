<?php

declare(strict_types=1);

namespace Workshop\App\Consumer;

use Workshop\Kafka\Runtime\TransientException;

/**
 * The Block 7 demo handler. Its only side effects are narration and the dedup
 * record the idempotency middleware adds around it — no projection table; the
 * demo is about the error path, not the happy path. While the failure-mode flag
 * is on (kafka:failure-mode on) it throws TransientException on every message,
 * standing in for a real dependency outage; a production handler would throw
 * the same marker from an actual timeout or 503.
 */
#[AsMessageHandler]
final readonly class ErrorDemoHandler
{
    public function __construct(
        private FailureModeSwitch $failureMode,
        private ConsoleWriter $console,
    ) {
    }

    public function __invoke(ErrorDemoDto $dto): void
    {
        if ($this->failureMode->enabled()) {
            throw new TransientException(sprintf('simulated transient outage (failure mode is on) — seq=%d id=%s', $dto->seq, $dto->id));
        }

        $this->console->writeln(sprintf('      applied error.demo seq=<info>%d</info> id=%s (%s)', $dto->seq, $dto->id, $dto->note));
    }
}
