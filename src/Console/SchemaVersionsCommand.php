<?php

declare(strict_types=1);

namespace Workshop\Console;

use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use Workshop\Kafka\Serde\SchemaRegistryClient;
use Workshop\Produce\MessageRouting;

#[AsCommand(
    name: 'schema:versions',
    description: 'List the registered schema versions for an event subject (shows the evolution lineage [1, 2, 3, …]).',
)]
final class SchemaVersionsCommand extends Command
{
    public function __construct(
        private readonly SchemaRegistryClient $registry,
        private readonly MessageRouting $routing,
    ) {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this->addArgument('type', InputArgument::REQUIRED, 'order-created | payment-processed | inventory-reserved');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $type = Input::string($input, 'type');
        if (! in_array($type, $this->routing->names(), true)) {
            $output->writeln('<error>Unknown event type. Use: order-created | payment-processed | inventory-reserved</error>');

            return Command::INVALID;
        }

        $subject = $this->routing->for($type)->subject;
        $versions = $this->registry->versions($subject);

        if ([] === $versions) {
            $output->writeln("subject <info>{$subject}</info>: <comment>not registered yet</comment>");
            $output->writeln('Register it: <comment>bin/console schema:register ' . $type . '</comment>');

            return Command::SUCCESS;
        }

        $output->writeln("subject <info>{$subject}</info>: [" . implode(', ', $versions) . ']');

        return Command::SUCCESS;
    }
}
