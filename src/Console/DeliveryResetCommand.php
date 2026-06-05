<?php

declare(strict_types=1);

namespace Workshop\Console;

use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Workshop\Kernel\Database;
use Workshop\Kernel\IdempotencyStore;
use Workshop\Kernel\SideEffectStore;

#[AsCommand(
    name: 'delivery:reset',
    description: 'Block 5 demo: truncate the side_effects and processed_events tables so the demo can be replayed from a clean slate.',
)]
final class DeliveryResetCommand extends Command
{
    public function __construct(
        private readonly Database $db,
        private readonly IdempotencyStore $idempotency,
        private readonly SideEffectStore $sideEffects,
    ) {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this
            ->addArgument('topic', InputArgument::OPTIONAL, 'Topic the demo consumes (used only for the offset-reset hint)', 'enet.ecommerce.orders')
            ->addOption('group', 'g', InputOption::VALUE_REQUIRED, 'Demo consumer group (used only for the offset-reset hint)', 'delivery-demo');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $this->db->ensureSchema();
        $conn = $this->db->connection();
        $this->sideEffects->truncate($conn);
        $this->idempotency->truncate($conn);

        $output->writeln('<info>truncated</info> side_effects + processed_events.');
        $output->writeln('');
        $output->writeln('<comment>To replay the same messages, also rewind the group offset:</comment>');
        $output->writeln(sprintf(
            '  bin/group-reset %s earliest %s',
            (string) $input->getOption('group'),
            (string) $input->getArgument('topic'),
        ));

        return Command::SUCCESS;
    }
}
