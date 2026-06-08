<?php

declare(strict_types=1);

namespace Workshop\Console;

use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use Workshop\Kafka\Serde\SchemaRegistrationException;
use Workshop\Kafka\Serde\SchemaRegistryClient;
use Workshop\Produce\MessageRouting;

#[AsCommand(
    name: 'schema:register',
    description: 'Register an event subject\'s AVRO schema with the Schema Registry — the explicit, out-of-band production path. The registry enforces compatibility and assigns the schema id producers embed in the wire format.',
)]
final class SchemaRegisterCommand extends Command
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
            ->addArgument('type', InputArgument::REQUIRED, 'order-created | order-updated | order-cancelled | payment-processed | inventory-reserved')
            ->addArgument('schema-file', InputArgument::OPTIONAL, 'Path to the .avsc to register (default: the subject\'s canonical schema from the routing table)');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $type = Input::string($input, 'type');
        if (! in_array($type, $this->routing->names(), true)) {
            $output->writeln('<error>Unknown event type. Use one of: ' . implode(' | ', $this->routing->names()) . '</error>');

            return Command::INVALID;
        }

        $route = $this->routing->for($type);
        $file = Input::stringOrNull($input, 'schema-file') ?? $route->schemaPath;

        $schemaJson = @file_get_contents($file);
        if (false === $schemaJson) {
            $output->writeln("<error>Cannot read schema file: {$file}</error>");

            return Command::INVALID;
        }

        if (null === json_decode($schemaJson)) {
            $output->writeln("<error>Schema file is not valid JSON: {$file}</error>");

            return Command::INVALID;
        }

        $output->writeln("subject <info>{$route->subject}</info> ← {$file}");

        try {
            $id = $this->registry->register($route->subject, $schemaJson);
        } catch (SchemaRegistrationException $e) {
            $output->writeln('  <error>✗ ' . $e->getMessage() . '</error>');

            return Command::FAILURE;
        }

        $output->writeln("  <info>✓ registered</info> as schema id <info>{$id}</info> — producers can now encode against it.");

        return Command::SUCCESS;
    }
}
