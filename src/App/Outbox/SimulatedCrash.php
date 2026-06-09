<?php

declare(strict_types=1);

namespace Workshop\App\Outbox;

/**
 * Thrown by OutboxPlacer when `outbox:place --fail` asks for a crash after both
 * writes but before COMMIT — the demo beat that proves the order row and its
 * outbox row live or die together. Its own type so the command can catch exactly
 * this simulation and narrate the rollback, while real failures still propagate.
 */
final class SimulatedCrash extends \RuntimeException
{
}
