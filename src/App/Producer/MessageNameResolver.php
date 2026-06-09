<?php

declare(strict_types=1);

namespace Workshop\App\Producer;

/**
 * Resolves a Message's wire name from its #[MessageName] attribute. Reflection
 * runs once per concrete class; the class-string => name mapping is memoized for
 * the rest of the runtime. Living here — at the produce/serialization stage —
 * keeps reflection off the per-message path: producing many messages of the same
 * type reflects only once, and the Message value objects stay reflection-free.
 */
final class MessageNameResolver
{
    /**
     * @var array<class-string, string>
     */
    private array $cache = [];

    public function nameOf(Message $message): string
    {
        return $this->cache[$message::class] ??= self::reflect($message::class);
    }

    /**
     * @param class-string $class
     */
    private static function reflect(string $class): string
    {
        $attributes = new \ReflectionClass($class)->getAttributes(MessageName::class);
        if ([] === $attributes) {
            throw new \LogicException(sprintf('%s is missing the #[MessageName] attribute.', $class));
        }

        return $attributes[0]->newInstance()->value;
    }
}
