<?php

declare(strict_types=1);

namespace Workshop\App\Console;

use Symfony\Component\Console\Input\InputInterface;

/**
 * Symfony Console returns arguments and options as mixed. These helpers narrow a
 * value (argument or option, resolved by name) to a concrete scalar in one place,
 * so commands stay free of repeated (string)/(int) casts on mixed — which static
 * analysis rightly rejects.
 */
final class Input
{
    public static function string(InputInterface $input, string $name): string
    {
        $value = self::value($input, $name);

        return is_scalar($value) ? (string) $value : '';
    }

    public static function int(InputInterface $input, string $name): int
    {
        $value = self::value($input, $name);

        return is_numeric($value) ? (int) $value : 0;
    }

    public static function stringOrNull(InputInterface $input, string $name): ?string
    {
        $value = self::value($input, $name);

        return is_string($value) ? $value : null;
    }

    public static function intOrNull(InputInterface $input, string $name): ?int
    {
        $value = self::value($input, $name);

        return is_numeric($value) ? (int) $value : null;
    }

    private static function value(InputInterface $input, string $name): mixed
    {
        return $input->hasArgument($name) ? $input->getArgument($name) : $input->getOption($name);
    }
}
