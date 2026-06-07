<?php

declare(strict_types=1);

namespace Workshop\Kafka\Callback;

/**
 * Composes a chosen set of ConfCallbacks and attaches them to a \RdKafka\Conf in
 * one call. Factories assemble the right kit per role (a consumer gets rebalance +
 * error; a producer gets delivery-report + error) so the command never wires raw
 * callbacks itself.
 */
final readonly class CallbackKit
{
    /**
     * @var list<ConfCallback>
     */
    private array $callbacks;

    public function __construct(ConfCallback ...$callbacks)
    {
        $this->callbacks = $callbacks;
    }

    public function attachTo(\RdKafka\Conf $conf): void
    {
        foreach ($this->callbacks as $callback) {
            $callback->attachTo($conf);
        }
    }
}
