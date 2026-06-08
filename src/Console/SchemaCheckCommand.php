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
    name: 'schema:check',
    description: 'Check whether a candidate .avsc is compatible with the latest registered version of an event subject — the pre-registration CI gate.',
)]
final class SchemaCheckCommand extends Command
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
            ->addArgument('type', InputArgument::REQUIRED, 'order-created | payment-processed | inventory-reserved')
            ->addArgument('schema-file', InputArgument::REQUIRED, 'Path to the candidate .avsc to test (e.g. schemas/orders/evolution/OrderCreated-v2-compatible.avsc)');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $type = Input::string($input, 'type');
        if (! in_array($type, $this->routing->names(), true)) {
            $output->writeln('<error>Unknown event type. Use: order-created | payment-processed | inventory-reserved</error>');

            return Command::INVALID;
        }

        $file = Input::string($input, 'schema-file');
        $schemaJson = @file_get_contents($file);
        if (false === $schemaJson) {
            $output->writeln("<error>Cannot read schema file: {$file}</error>");

            return Command::INVALID;
        }

        if (null === json_decode($schemaJson)) {
            $output->writeln("<error>Schema file is not valid JSON: {$file}</error>");

            return Command::INVALID;
        }

        $subject = $this->routing->for($type)->subject;
        $result = $this->registry->checkCompatibility($subject, $schemaJson);

        $output->writeln("subject <info>{$subject}</info> ← {$file}");

        if ($result['firstVersion']) {
            $output->writeln('  <comment>no version registered yet</comment> — first schema is always accepted, nothing to check.');
            $output->writeln('  Register it by producing once: <comment>bin/console events:produce ' . $type . '</comment>');

            return Command::SUCCESS;
        }

        if ($result['compatible']) {
            $output->writeln('  <info>✓ COMPATIBLE</info> with the latest registered version — safe to register.');

            return Command::SUCCESS;
        }

        $output->writeln('  <error>✗ NOT COMPATIBLE</error> with the latest registered version — registration would be rejected (409).');
        $output->writeln('  Fix: add a <comment>default</comment> to new fields, or avoid removing/renaming/retyping required fields.');

        return Command::FAILURE;
    }
}
