<?php

declare(strict_types=1);

namespace Workshop\Kernel;

enum Topics: string
{
    case ConsumerGroups = 'consumer-groups-events';
    case Offsets        = 'offsets-events';
    case Partitioning   = 'partitioning-events';
}
