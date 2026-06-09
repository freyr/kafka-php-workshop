<?php

declare(strict_types=1);

namespace Workshop\App\Console;

use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Workshop\App\Outbox\PayloadFormat;
use Workshop\Framework\Db\OutboxSchemaInstaller;
use Workshop\Framework\Db\SchemaInstaller;

#[AsCommand(
    name: 'outbox:setup',
    description: 'Provision the Block 6 outbox table (and ensure the orders table outbox:place mutates). Idempotent — safe to re-run.',
)]
final class OutboxSetupCommand extends Command
{
    public function __construct(
        private readonly OutboxSchemaInstaller $installer,
        private readonly SchemaInstaller $projectionInstaller,
    ) {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this
            ->addOption('fresh', null, InputOption::VALUE_NONE, 'Drop the outbox first, then recreate it empty — a full outbox reset. The orders projection is only ensured, never dropped here (that reset is kafka:consume:setup --fresh)')
            ->addOption('format', null, InputOption::VALUE_REQUIRED, 'Payload format the table is provisioned for: json (human-readable envelope, Debezium expands it) | avro (Confluent-framed bytes against the registered schemas). Switching formats requires --fresh', 'json');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        try {
            $format = PayloadFormat::fromOption(Input::string($input, 'format'));
        } catch (\InvalidArgumentException $e) {
            $output->writeln('<error>' . $e->getMessage() . '</error>');

            return Command::INVALID;
        }

        if ((bool) $input->getOption('fresh')) {
            foreach ($this->installer->drop() as $table) {
                $output->writeln(sprintf('  <comment>✗</comment> %s dropped', $table));
            }
        }

        // A format switch must be explicit: re-running setup never silently
        // changes the payload column under existing rows (CREATE IF NOT EXISTS
        // would not change it anyway — it would just lie about the format).
        $current = $this->installer->payloadColumnType();
        if (null !== $current && $current !== $format->columnType()) {
            $output->writeln(sprintf('<error>outbox already provisioned with a %s payload column — switching to %s needs an explicit reset:</error>', $current, $format->value));
            $output->writeln(sprintf('  <comment>bin/console outbox:setup --fresh --format %s</comment>', $format->value));

            return Command::FAILURE;
        }

        // outbox:place writes the order row and the outbox row in one transaction,
        // so both tables must exist; ensuring (not resetting) the projection store
        // here keeps `outbox:setup` the only prerequisite for the Block 6 demo.
        foreach ($this->projectionInstaller->install() as $table) {
            $output->writeln(sprintf('  <info>✓</info> %s', $table));
        }
        foreach ($this->installer->install($format) as $table) {
            $output->writeln(sprintf('  <info>✓</info> %s (payload: %s)', $table, $format->value));
        }

        $output->writeln('<info>outbox store ready</info>');

        return Command::SUCCESS;
    }
}
