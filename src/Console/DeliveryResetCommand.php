<?php

declare(strict_types=1);

namespace Workshop\Console;

use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Workshop\Kernel\IdempotencyStore;
use Workshop\Kernel\SideEffectLog;

#[AsCommand(
    name: 'delivery:reset',
    description: 'Block 5 demo: clear the side-effect log and idempotency store so the demo can be replayed from a clean slate.',
)]
final class DeliveryResetCommand extends Command
{
    public function __construct(
        private readonly IdempotencyStore $store,
        private readonly SideEffectLog $sideEffects,
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
        $this->sideEffects->reset();
        $this->store->reset();

        $output->writeln('<info>cleared</info> side-effect log + idempotency store.');
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
