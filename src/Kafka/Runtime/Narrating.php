<?php

declare(strict_types=1);

namespace Workshop\Kafka\Runtime;

/**
 * Shared optional-narration helper. A callback narrates human-readable lines only
 * when a sink is supplied (the verbose/Block-1 mode); otherwise it stays silent.
 */
trait Narrating
{
    private function narrate(string $line): void
    {
        if (null !== $this->narrate) {
            ($this->narrate)($line);
        }
    }
}
