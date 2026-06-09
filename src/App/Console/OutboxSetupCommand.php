<?php

declare(strict_types=1);

namespace Workshop\App\Console;

use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
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
        $this->addOption('fresh', null, InputOption::VALUE_NONE, 'Drop the outbox first, then recreate it empty — a full outbox reset. The orders projection is only ensured, never dropped here (that reset is kafka:consume:setup --fresh)');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        if ((bool) $input->getOption('fresh')) {
            foreach ($this->installer->drop() as $table) {
                $output->writeln(sprintf('  <comment>✗</comment> %s dropped', $table));
            }
        }

        // outbox:place writes the order row and the outbox row in one transaction,
        // so both tables must exist; ensuring (not resetting) the projection store
        // here keeps `outbox:setup` the only prerequisite for the Block 6 demo.
        foreach ($this->projectionInstaller->install() as $table) {
            $output->writeln(sprintf('  <info>✓</info> %s', $table));
        }
        foreach ($this->installer->install() as $table) {
            $output->writeln(sprintf('  <info>✓</info> %s', $table));
        }

        $output->writeln('<info>outbox store ready</info>');

        return Command::SUCCESS;
    }
}
