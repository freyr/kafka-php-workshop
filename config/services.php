<?php

declare(strict_types=1);

use Symfony\Component\Console\Command\Command;
use Symfony\Component\DependencyInjection\Loader\Configurator\ContainerConfigurator;
use Workshop\Console\ConfigStatsCommand;
use Workshop\Kafka\Config\BrokerProbe;
use Workshop\Kafka\Config\ConfBuilder;
use Workshop\Kafka\Config\TcpBrokerProbe;
use Workshop\Kernel\AvroEventSerializer;
use Workshop\Kernel\Database;
use Workshop\Kernel\KafkaContextFactory;
use Workshop\Kernel\SchemaRegistryClient;

return function (ContainerConfigurator $c): void {
    $services = $c->services()
        ->defaults()
        ->autowire()
        ->autoconfigure();

    // Any Symfony Command discovered below is tagged so bin/console picks it up,
    // and made public so we can fetch it from the compiled container by id.
    $services->instanceof(Command::class)
        ->tag('console.command')
        ->public();

    // PSR-4 autodiscovery — every class under these namespaces becomes a service.
    // Autowire resolves constructor dependencies by type.
    // Enums are value objects, not services — exclude them from autodiscovery.
    $services->load('Workshop\\Kernel\\', __DIR__ . '/../src/Kernel/')
        ->exclude([
            __DIR__ . '/../src/Kernel/Topics.php',
            __DIR__ . '/../src/Kernel/WorkshopEvent.php',
        ]);

    // The pure php-rdkafka layer (Blocks 1-3). Concept-named sub-namespaces under
    // src/Kafka/ become autowired services; the value objects and enums are
    // constructed, never injected, so they are excluded like Topics/WorkshopEvent.
    $services->load('Workshop\\Kafka\\', __DIR__ . '/../src/Kafka/')
        ->exclude([
            __DIR__ . '/../src/Kafka/Config/ClientRole.php',
            __DIR__ . '/../src/Kafka/Config/KafkaSetting.php',
            __DIR__ . '/../src/Kafka/Config/KafkaProfile.php',
            __DIR__ . '/../src/Kafka/Runtime/CommitPolicy.php',
            __DIR__ . '/../src/Kafka/Runtime/RunLimits.php',
        ]);

    $services->load('Workshop\\Console\\', __DIR__ . '/../src/Console/');

    // KafkaContextFactory takes a string broker list, which can't be type-hint-
    // autowired. Bind the named arg to the container parameter.
    $services->set(KafkaContextFactory::class)
        ->arg('$brokers', '%kafka.brokers%');

    // AvroEventSerializer takes the Schema Registry URL — bind the named arg.
    $services->set(AvroEventSerializer::class)
        ->arg('$schemaRegistryUrl', '%schema_registry.url%');

    // SchemaRegistryClient (Block 4 tooling) also takes the Schema Registry URL.
    $services->set(SchemaRegistryClient::class)
        ->arg('$schemaRegistryUrl', '%schema_registry.url%');

    // Database (Block 5 idempotency demo) takes the DBAL connection URL.
    $services->set(Database::class)
        ->arg('$url', '%database.url%');

    // ConfigStatsCommand (Block 8 raw-rdkafka deep dive) needs the broker list as
    // a string — same non-autowirable case as KafkaContextFactory.
    $services->set(ConfigStatsCommand::class)
        ->arg('$brokers', '%kafka.brokers%');

    // The pure-rdkafka ConfBuilder needs the broker list as a string; the broker
    // probe interface resolves to its TCP implementation.
    $services->set(ConfBuilder::class)
        ->arg('$brokers', '%kafka.brokers%');
    $services->alias(BrokerProbe::class, TcpBrokerProbe::class);
};
