<?php

declare(strict_types=1);

namespace Workshop\App\Console;

use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use Workshop\App\Consumer\FailureModeSwitch;

#[AsCommand(
    name: 'kafka:failure-mode',
    description: 'Flip the Block 7 transient-failure switch: while ON, the error.demo handler throws TransientException on every message — the running consumer starts retrying immediately.',
)]
final class FailureModeCommand extends Command
{
    public function __construct(
        private readonly FailureModeSwitch $switch,
    ) {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this->addArgument('action', InputArgument::REQUIRED, 'on (handler starts throwing) | off (system restored) | status');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $action = Input::string($input, 'action');

        switch ($action) {
            case 'on':
                $this->switch->enable();
                $output->writeln('<comment>failure mode ON</comment> — the error.demo handler now throws TransientException on every message; a running consumer picks this up on its next message');

                return Command::SUCCESS;

            case 'off':
                $this->switch->disable();
                $output->writeln('<info>failure mode OFF</info> — the simulated outage is over; retries start succeeding on their next attempt');

                return Command::SUCCESS;

            case 'status':
                $output->writeln($this->switch->enabled()
                    ? '<comment>failure mode is ON</comment> (transient outage simulated)'
                    : '<info>failure mode is OFF</info> (handlers succeed)');

                return Command::SUCCESS;

            default:
                $output->writeln(sprintf('<error>Unknown action "%s".</error> Use: on | off | status', $action));

                return Command::INVALID;
        }
    }
}
