<?php

declare(strict_types=1);

namespace Workshop\App\Producer;

/**
 * Maps a message name to its wire identity (topic + subject + schema). The single
 * source of truth for produce-side routing, loaded as data from
 * config/producers.yaml, not code.
 */
final readonly class MessageRouting
{
    /**
     * @param array<string, array{topic: string, subject?: string, schema?: string}> $routes
     */
    public function __construct(
        private array $routes,
    ) {
    }

    public function for(string $name): Route
    {
        if (! isset($this->routes[$name])) {
            throw new \InvalidArgumentException("No produce route configured for message '{$name}'.");
        }

        $route = $this->routes[$name];

        return new Route($route['topic'], $route['subject'] ?? '', $route['schema'] ?? '');
    }

    /**
     * @return list<string>
     */
    public function names(): array
    {
        return array_keys($this->routes);
    }
}
