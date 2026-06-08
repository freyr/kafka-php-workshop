<?php

declare(strict_types=1);

namespace Workshop\Console;

use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Workshop\Kafka\Serde\SchemaRegistrationException;
use Workshop\Kafka\Serde\SchemaRegistryClient;
use Workshop\Produce\MessageRouting;

#[AsCommand(
    name: 'schema:register',
    description: 'Register an event subject\'s AVRO schema with the Schema Registry — the explicit, out-of-band production path. The registry enforces compatibility and assigns the schema id producers embed in the wire format. Use --all to register every routed subject in one shot.',
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
            ->addArgument('type', InputArgument::OPTIONAL, 'order.created | order.updated | order.cancelled | payment.processed | inventory.reserved (omit with --all)')
            ->addArgument('schema-file', InputArgument::OPTIONAL, 'Path to the .avsc to register (default: the subject\'s canonical schema from the routing table)')
            ->addOption('all', null, InputOption::VALUE_NONE, 'Register every routed subject\'s canonical schema — the one-shot bootstrap for a fresh stack');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $type = Input::stringOrNull($input, 'type');
        $file = Input::stringOrNull($input, 'schema-file');

        if (true === $input->getOption('all')) {
            return $this->registerAll($type, $file, $output);
        }

        if (null === $type) {
            $output->writeln('<error>Specify an event type, or pass --all to register every subject.</error>');

            return Command::INVALID;
        }

        if (! in_array($type, $this->routing->names(), true)) {
            $output->writeln('<error>Unknown event type. Use one of: ' . implode(' | ', $this->routing->names()) . '</error>');

            return Command::INVALID;
        }

        return $this->registerSubject($type, $file, $output);
    }

    private function registerAll(?string $type, ?string $file, OutputInterface $output): int
    {
        if (null !== $type || null !== $file) {
            $output->writeln('<error>--all registers every subject\'s canonical schema; pass it alone, without a type or schema-file.</error>');

            return Command::INVALID;
        }

        $names = $this->routing->names();
        $failed = 0;
        foreach ($names as $name) {
            if (Command::SUCCESS !== $this->registerSubject($name, null, $output)) {
                ++$failed;
            }
        }

        $output->writeln('');
        if ($failed > 0) {
            $output->writeln(sprintf('<error>%d of %d subjects failed to register.</error>', $failed, count($names)));

            return Command::FAILURE;
        }

        $output->writeln(sprintf('<info>✓ all %d subjects registered.</info>', count($names)));

        return Command::SUCCESS;
    }

    private function registerSubject(string $type, ?string $file, OutputInterface $output): int
    {
        $route = $this->routing->for($type);
        $file ??= $route->schemaPath;

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

        $output->writeln("  <info>✓ registered</info> as schema id <info>{$id}</info>.");

        return Command::SUCCESS;
    }
}
