<?php

declare(strict_types=1);

namespace Workshop\App\Console;

use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use Workshop\Framework\Db\SchemaInstaller;

#[AsCommand(
    name: 'kafka:consume:setup',
    description: 'Provision the consumer store (orders projection + processed_events dedup ledger). Idempotent — safe to re-run.',
)]
final class ProjectionSetupCommand extends Command
{
    public function __construct(
        private readonly SchemaInstaller $installer,
    ) {
        parent::__construct();
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        foreach ($this->installer->install() as $table) {
            $output->writeln(sprintf('  <info>✓</info> %s', $table));
        }

        $output->writeln('<info>consumer store ready</info>');

        return Command::SUCCESS;
    }
}
