<?php

declare(strict_types=1);

namespace Workshop\Framework;

use Symfony\Component\Config\FileLocator;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Loader\YamlFileLoader;
use Workshop\Framework\DependencyInjection\WorkshopExtension;

/**
 * The thin framework layer: boots a compiled DI container from YAML. It does
 * three things and nothing more:
 *
 *  - seeds runtime parameters from the environment,
 *  - registers the one application extension, so the `workshop:` config keys in
 *    the YAML have an owner that turns them into services,
 *  - loads the YAML service definitions and compiles the container.
 *
 * Everything else (autowiring, autodiscovery, routing) is expressed as data in
 * config/*.yaml rather than wiring code.
 */
final readonly class Kernel
{
    public function __construct(
        private string $projectDir,
    ) {
    }

    /**
     * @throws \Exception
     */
    public function boot(): ContainerBuilder
    {
        $container = new ContainerBuilder();

        // $_ENV values are typed as mixed; narrow each to a string with a default.
        $env = static fn (string $key, string $default): string => is_string($value = $_ENV[$key] ?? null) ? $value : $default;

        $container->setParameter('kafka.project_dir', $this->projectDir);
        $container->setParameter('kafka.schema_dir', $this->projectDir . '/schemas');
        $container->setParameter('kafka.brokers', $env('KAFKA_BROKERS', 'kafka:29092'));
        $container->setParameter('schema_registry.url', $env('SCHEMA_REGISTRY_URL', 'http://schema-registry:8081'));
        $container->setParameter('database.url', $env('DATABASE_URL', 'mysql://workshop:workshop@mysql:3306/workshop?charset=utf8mb4'));

        // Registering the extension makes `workshop:` a valid top-level key in the
        // YAML; the loader hands those configs to WorkshopExtension at compile time.
        $container->registerExtension(new WorkshopExtension());

        new YamlFileLoader(
            $container,
            new FileLocator($this->projectDir . '/config')
        )->load('services.yaml');

        $container->compile();

        return $container;
    }
}
