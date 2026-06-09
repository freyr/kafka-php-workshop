<?php

declare(strict_types=1);

namespace Workshop\Framework;

use Symfony\Component\Config\FileLocator;
use Symfony\Component\DependencyInjection\ChildDefinition;
use Symfony\Component\DependencyInjection\Compiler\PassConfig;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Loader\YamlFileLoader;
use Workshop\App\Consumer\AsMessageHandler;
use Workshop\Framework\DependencyInjection\MessageHandlerPass;
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

        // Consumer handlers declare #[AsMessageHandler]; autoconfiguration turns that
        // attribute into the tag MessageHandlerPass collects, so a handler never names
        // the tag itself. The pass then reflects each handler's __invoke to build the
        // MessageBus routing table — run after the built-in instanceof/attribute
        // resolution (negative priority) so the tags are already in place.
        $container->registerAttributeForAutoconfiguration(
            AsMessageHandler::class,
            static function (ChildDefinition $definition, AsMessageHandler $attribute, \Reflector $reflector): void {
                $definition->addTag(MessageHandlerPass::TAG);
            },
        );
        $container->addCompilerPass(new MessageHandlerPass(), PassConfig::TYPE_BEFORE_OPTIMIZATION, -16);

        new YamlFileLoader(
            $container,
            new FileLocator($this->projectDir . '/config')
        )->load('services.yaml');

        $container->compile();

        return $container;
    }
}
