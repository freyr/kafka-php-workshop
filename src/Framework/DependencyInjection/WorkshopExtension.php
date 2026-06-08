<?php

declare(strict_types=1);

namespace Workshop\Framework\DependencyInjection;

use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Definition;
use Symfony\Component\DependencyInjection\Extension\Extension;
use Workshop\Consume\DtoRouting;
use Workshop\Produce\MessageRouting;

/**
 * Turns the validated `workshop:` config into the two routing services. The
 * tables themselves live in data (producers.yaml / consumers.yaml); this
 * extension is the only code that knows how that data becomes wiring. Parameter
 * placeholders in the values (e.g. %kafka.schema_dir%) survive into the service
 * arguments and are resolved during container compilation.
 */
final class WorkshopExtension extends Extension
{
    /**
     * @param array<array-key, mixed> $configs
     */
    public function load(array $configs, ContainerBuilder $container): void
    {
        $config = $this->processConfiguration(new Configuration(), $configs);

        $container->setDefinition(
            MessageRouting::class,
            new Definition(MessageRouting::class, [
                '$routes' => $config['producers'] ?? [],
            ]),
        );

        $container->setDefinition(
            DtoRouting::class,
            new Definition(DtoRouting::class, [
                '$map' => $config['consumers'] ?? [],
            ]),
        );
    }

    public function getAlias(): string
    {
        return 'workshop';
    }
}
