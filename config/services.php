<?php

declare(strict_types=1);

use Symfony\Component\Console\Command\Command;
use Symfony\Component\DependencyInjection\Loader\Configurator\ContainerConfigurator;
use Workshop\Kernel\AvroEventSerializer;
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
};
