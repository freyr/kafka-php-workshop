<?php

declare(strict_types=1);

namespace Workshop\App\Console;

use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Workshop\Framework\Db\CatalogSchemaInstaller;

#[AsCommand(
    name: 'catalog:setup',
    description: 'Provision the Block 9 projection demo: product_catalog_state_change (Debezium source) + products_projection (JDBC sink target). Idempotent — safe to re-run.',
)]
final class CatalogSetupCommand extends Command
{
    public function __construct(
        private readonly CatalogSchemaInstaller $installer,
    ) {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this->addOption('fresh', null, InputOption::VALUE_NONE, 'Drop both tables first, then recreate them empty — a full catalog reset instead of the default ensure-only run');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        if ((bool) $input->getOption('fresh')) {
            foreach ($this->installer->drop() as $table) {
                $output->writeln(sprintf('  <comment>✗</comment> %s dropped', $table));
            }
        }

        foreach ($this->installer->install() as $table) {
            $output->writeln(sprintf('  <info>✓</info> %s', $table));
        }

        $output->writeln('<info>catalog demo store ready</info>');

        return Command::SUCCESS;
    }
}
