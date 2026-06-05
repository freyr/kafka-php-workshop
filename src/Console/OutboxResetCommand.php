<?php

declare(strict_types=1);

namespace Workshop\Console;

use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use Workshop\Kernel\Database;
use Workshop\Kernel\OutboxStore;

#[AsCommand(
    name: 'outbox:reset',
    description: 'Block 6 demo: truncate the orders and outbox tables so the demo can be replayed from a clean slate.',
)]
final class OutboxResetCommand extends Command
{
    public function __construct(
        private readonly Database $db,
        private readonly OutboxStore $outbox,
    ) {
        parent::__construct();
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $this->db->ensureOutboxSchema();
        $conn = $this->db->connection();

        $conn->executeStatement('TRUNCATE TABLE orders');
        $this->outbox->truncate($conn);

        $output->writeln('<info>truncated</info> orders + outbox.');
        $output->writeln('');
        $output->writeln('<comment>Kafka topics are not touched — to re-read published events, consume from a fresh group:</comment>');
        $output->writeln('  bin/console events:consume enet.ecommerce.orders          # ephemeral group, from earliest');

        return Command::SUCCESS;
    }
}
