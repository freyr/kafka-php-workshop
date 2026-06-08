<?php

declare(strict_types=1);

namespace Workshop\Kafka\Config;

enum Topics: string
{
    case ConsumerGroups = 'consumer-groups-events';
    case Offsets = 'offsets-events';
    case Partitioning = 'partitioning-events';
}
