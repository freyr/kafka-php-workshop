<?php

declare(strict_types=1);

namespace Workshop\Kafka\Callback;

use RdKafka\Conf;

/**
 * One librdkafka callback that knows how to attach itself to a \RdKafka\Conf. The
 * interesting part of a raw client is not its string properties but its callbacks
 * — rebalance, stats, error, delivery-report — so each is modelled as a small,
 * reusable unit instead of an inline closure buried in a command.
 */
interface ConfCallback
{
    public function attachTo(Conf $conf): void;
}
