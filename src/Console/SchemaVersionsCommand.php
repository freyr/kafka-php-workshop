<?php

declare(strict_types=1);

namespace Workshop\Console;

use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use Workshop\Kernel\SchemaRegistryClient;
use Workshop\Kernel\WorkshopEvent;

#[AsCommand(
    name: 'schema:versions',
    description: 'List the registered schema versions for an event subject (shows the evolution lineage [1, 2, 3, …]).',
)]
final class SchemaVersionsCommand extends Command
{
    public function __construct(
        private readonly SchemaRegistryClient $registry,
    ) {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this->addArgument('type', InputArgument::REQUIRED, 'order-created | payment-processed | inventory-reserved');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $type = WorkshopEvent::tryFrom(Input::string($input, 'type'));
        if (null === $type) {
            $output->writeln('<error>Unknown event type. Use: order-created | payment-processed | inventory-reserved</error>');

            return Command::INVALID;
        }

        $subject = $type->subject();
        $versions = $this->registry->versions($subject);

        if ([] === $versions) {
            $output->writeln("subject <info>{$subject}</info>: <comment>not registered yet</comment>");
            $output->writeln('Register it by producing once: <comment>bin/console events:produce ' . $type->value . '</comment>');

            return Command::SUCCESS;
        }

        $output->writeln("subject <info>{$subject}</info>: [" . implode(', ', $versions) . ']');

        return Command::SUCCESS;
    }
}
