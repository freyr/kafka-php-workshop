<?php

declare(strict_types=1);

namespace Workshop\App\Consumer;

/**
 * Marks an invokable consumer handler. The DTO it handles is not declared here —
 * it is inferred from the type of the handler's `__invoke` first parameter (e.g.
 * `__invoke(OrderCreatedDto $dto)` handles `OrderCreatedDto`). MessageHandlerPass
 * reads that signature at container-build time to wire the routing table, so a
 * handler's only job is to name the DTO in its signature and do the work.
 *
 * Exactly one handler may claim a given DTO; a second is a container-build error.
 */
#[\Attribute(\Attribute::TARGET_CLASS)]
final class AsMessageHandler
{
}
