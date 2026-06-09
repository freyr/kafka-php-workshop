<?php

declare(strict_types=1);

namespace Workshop\App\Console\Schema;

use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use Workshop\App\Console\Input;
use Workshop\App\Producer\MessageRouting;
use Workshop\Kafka\Serde\SchemaRegistrationException;
use Workshop\Kafka\Serde\SchemaRegistryClient;

#[AsCommand(
    name: 'kafka:schema:delete',
    description: 'Delete a registered schema version of a subject (default: the latest). Used in the evolution exercise to drop a just-registered version before re-registering it under a stricter compatibility level.',
)]
final class SchemaDeleteCommand extends Command
{
    public function __construct(
        private readonly SchemaRegistryClient $registry,
        private readonly MessageRouting $routing,
    ) {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this
            ->addArgument('type', InputArgument::REQUIRED, implode(' | ', $this->routing->names()))
            ->addArgument('version', InputArgument::OPTIONAL, 'Version number to delete; omit for the latest', 'latest');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $type = Input::string($input, 'type');
        if (! in_array($type, $this->routing->names(), true)) {
            $output->writeln('<error>Unknown event type. Use one of: ' . implode(' | ', $this->routing->names()) . '</error>');

            return Command::INVALID;
        }

        $subject = $this->routing->for($type)->subject;
        $version = Input::string($input, 'version');

        try {
            $deleted = $this->registry->deleteVersion($subject, $version);
        } catch (SchemaRegistrationException $e) {
            $output->writeln('  <error>✗ ' . $e->getMessage() . '</error>');

            return Command::FAILURE;
        }

        if (null === $deleted) {
            $output->writeln(sprintf('subject <info>%s</info>: <comment>nothing to delete</comment> (version %s not found)', $subject, $version));

            return Command::SUCCESS;
        }

        $output->writeln(sprintf('subject <info>%s</info>: deleted version <info>%d</info>', $subject, $deleted));

        return Command::SUCCESS;
    }
}
