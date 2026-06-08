<?php

declare(strict_types=1);

namespace Workshop\Framework\DependencyInjection;

use Symfony\Component\Config\Definition\Builder\TreeBuilder;
use Symfony\Component\Config\Definition\ConfigurationInterface;

/**
 * The contract for the `workshop:` config namespace — the producer and consumer
 * routing tables. The tree is the validation: a producer must declare a topic and
 * a type (avro | json); an avro producer must additionally declare a subject and a
 * schema, a json producer needs neither. A consumer maps a message name to its
 * read-model DTO. A malformed producers.yaml / consumers.yaml fails fast at
 * container compile, not at produce/consume time.
 */
final class Configuration implements ConfigurationInterface
{
    public function getConfigTreeBuilder(): TreeBuilder
    {
        $treeBuilder = new TreeBuilder('workshop');

        $treeBuilder->getRootNode()
            ->children()
            ->arrayNode('producers')
            ->useAttributeAsKey('name')
            ->arrayPrototype()
            ->children()
            ->enumNode('type')->values(['avro', 'json'])->defaultValue('avro')->end()
            ->scalarNode('topic')->isRequired()->cannotBeEmpty()->end()
            ->scalarNode('subject')->defaultValue('')->end()
            ->scalarNode('schema')->defaultValue('')->end()
            ->end()
            ->validate()
            ->ifTrue(static fn (array $route): bool => 'avro' === $route['type'] && ('' === $route['subject'] || '' === $route['schema']))
            ->thenInvalid('An avro producer route requires a non-empty subject and schema.')
            ->end()
            ->end()
            ->end()
            ->arrayNode('consumers')
            ->useAttributeAsKey('name')
            ->scalarPrototype()->cannotBeEmpty()->end()
            ->end()
            ->end();

        return $treeBuilder;
    }
}
