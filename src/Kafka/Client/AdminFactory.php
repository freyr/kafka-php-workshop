<?php

declare(strict_types=1);

namespace Workshop\Kafka\Client;

use Workshop\Kafka\Admin\TopicAdmin;
use Workshop\Kafka\Config\ClientRole;
use Workshop\Kafka\Config\ConfBuilder;
use Workshop\Kafka\Config\KafkaProfile;

/**
 * Builds a TopicAdmin. The third role factory: admin needs no group, no
 * idempotence, no rebalance handler — just a lightweight client for metadata
 * reads, so it is built from a bare admin profile (client.id = workshop.admin.<pid>).
 */
final readonly class AdminFactory
{
    public function __construct(
        private ConfBuilder $confBuilder,
    ) {
    }

    public function create(): TopicAdmin
    {
        $conf = $this->confBuilder->build(new KafkaProfile('admin', ClientRole::Admin, []));

        return new TopicAdmin(new \RdKafka\Producer($conf));
    }
}
