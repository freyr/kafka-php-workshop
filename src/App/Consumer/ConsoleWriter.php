<?php

declare(strict_types=1);

namespace Workshop\App\Consumer;

use Symfony\Component\Console\Output\OutputInterface;

/**
 * A console sink for handlers that print. A bus handler is a DI service with no
 * access to the running command's output, so a printing handler (FieldPrintHandler)
 * writes through this holder instead. The command binds its real output once at
 * startup; until then writes are dropped. One CLI process = one run, so the shared
 * mutable output is safe — and tests can bind a BufferedOutput to capture it.
 */
final class ConsoleWriter
{
    private ?OutputInterface $output = null;

    public function bind(OutputInterface $output): void
    {
        $this->output = $output;
    }

    public function writeln(string $line): void
    {
        $this->output?->writeln($line);
    }
}
