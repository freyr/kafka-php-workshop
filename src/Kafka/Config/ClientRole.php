<?php

declare(strict_types=1);

namespace Workshop\Kafka\Config;

/**
 * The three roles a Kafka client can take, mirroring the librdkafka object model:
 * a producer writes, a consumer reads under a group, an admin inspects the
 * cluster. A KafkaProfile is bound to exactly one role so a producer-only setting
 * can never leak onto a consumer (and vice versa) — the asymmetry is structural,
 * not a runtime check.
 */
enum ClientRole: string
{
    case Producer = 'producer';
    case Consumer = 'consumer';
    case Admin = 'admin';
}
