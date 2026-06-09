<?php

declare(strict_types=1);

namespace Workshop\Framework\DependencyInjection;

use Symfony\Component\DependencyInjection\Compiler\CompilerPassInterface;
use Symfony\Component\DependencyInjection\Compiler\ServiceLocatorTagPass;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Reference;
use Workshop\App\Consumer\MessageBus;

/**
 * Builds the MessageBus routing table from the tagged #[AsMessageHandler] services.
 * For each handler it reflects the type of its `__invoke` first parameter — the DTO
 * the handler claims — and registers it under that class-string in a service
 * locator handed to the MessageBus. Two handlers claiming the same DTO is a build
 * error here, which is how "exactly one handler per DTO" is enforced: the wiring
 * fails to compile rather than picking a winner at runtime.
 *
 * The tag itself is applied by attribute autoconfiguration registered in the Kernel,
 * so a handler only declares the attribute and the DTO in its signature — never the
 * tag name or the routing key.
 */
final class MessageHandlerPass implements CompilerPassInterface
{
    public const string TAG = 'workshop.message_handler';

    public function process(ContainerBuilder $container): void
    {
        if (! $container->hasDefinition(MessageBus::class)) {
            return;
        }

        /** @var array<class-string, string> $owners DTO class => owning service id */
        $owners = [];
        /** @var array<class-string, Reference> $refs DTO class => handler reference */
        $refs = [];

        foreach (array_keys($container->findTaggedServiceIds(self::TAG)) as $id) {
            $class = $container->getDefinition($id)->getClass() ?? $id;
            $dtoClass = $this->dtoClassFor($class, $id);

            if (isset($owners[$dtoClass])) {
                throw new \LogicException(sprintf('Two message handlers claim %s: "%s" and "%s". A DTO may have exactly one handler.', $dtoClass, $owners[$dtoClass], $id));
            }

            $owners[$dtoClass] = $id;
            $refs[$dtoClass] = new Reference($id);
        }

        $locator = ServiceLocatorTagPass::register($container, $refs);
        $container->getDefinition(MessageBus::class)->setArgument('$handlers', $locator);
    }

    /**
     * @return class-string the DTO named by the handler's __invoke first parameter
     */
    private function dtoClassFor(string $class, string $id): string
    {
        if (! class_exists($class)) {
            throw new \LogicException(sprintf('Message handler "%s" maps to a missing class "%s".', $id, $class));
        }

        $reflection = new \ReflectionClass($class);
        if (! $reflection->hasMethod('__invoke')) {
            throw new \LogicException(sprintf('Message handler "%s" must declare __invoke(DtoType $dto).', $id));
        }

        $parameters = $reflection->getMethod('__invoke')->getParameters();
        if ([] === $parameters) {
            throw new \LogicException(sprintf('Message handler "%s" __invoke must accept the DTO as its first parameter.', $id));
        }

        $type = $parameters[0]->getType();
        if (! $type instanceof \ReflectionNamedType || $type->isBuiltin()) {
            throw new \LogicException(sprintf('Message handler "%s" __invoke first parameter must be typed with a DTO class.', $id));
        }

        /** @var class-string */
        return $type->getName();
    }
}
