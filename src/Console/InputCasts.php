<?php

declare(strict_types=1);

namespace Workshop\Console;

use Symfony\Component\Console\Input\InputInterface;

/**
 * Symfony Console returns arguments and options as mixed. These helpers narrow
 * them to concrete scalar types in one place so the commands stay free of repeated
 * (string)/(int) casts on mixed (which static analysis rightly rejects).
 */
trait InputCasts
{
    private function argString(InputInterface $input, string $name): string
    {
        $value = $input->getArgument($name);

        return is_scalar($value) ? (string) $value : '';
    }

    private function optString(InputInterface $input, string $name): ?string
    {
        $value = $input->getOption($name);
        if (null === $value) {
            return null;
        }

        return is_scalar($value) ? (string) $value : null;
    }

    private function optInt(InputInterface $input, string $name): int
    {
        $value = $input->getOption($name);

        return is_numeric($value) ? (int) $value : 0;
    }

    private function optIntOrNull(InputInterface $input, string $name): ?int
    {
        $value = $input->getOption($name);

        return is_numeric($value) ? (int) $value : null;
    }
}
