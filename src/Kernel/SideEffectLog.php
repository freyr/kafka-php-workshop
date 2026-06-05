<?php

declare(strict_types=1);

namespace Workshop\Kernel;

/**
 * A file-backed stand-in for a real, observable side-effect — "charge the card",
 * "send the email", "reserve the stock". Each applied effect appends one line to
 * var/delivery-side-effects.log so the Block 5 demo can show, by counting lines,
 * whether a redelivered message produced a duplicate side-effect or not.
 */
final readonly class SideEffectLog
{
    private string $file;

    public function __construct()
    {
        $dir = dirname(__DIR__, 2) . '/var';
        if (! is_dir($dir)) {
            @mkdir($dir, 0o777, true);
        }

        $this->file = $dir . '/delivery-side-effects.log';
    }

    public function append(string $line): void
    {
        file_put_contents($this->file, $line . PHP_EOL, FILE_APPEND);
    }

    public function reset(): void
    {
        if (is_file($this->file)) {
            unlink($this->file);
        }
    }

    public function count(): int
    {
        if (! is_file($this->file)) {
            return 0;
        }

        $lines = file($this->file, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES);

        return false === $lines ? 0 : \count($lines);
    }

    public function path(): string
    {
        return $this->file;
    }
}
