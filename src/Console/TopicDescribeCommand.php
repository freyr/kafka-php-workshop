<?php

declare(strict_types=1);

namespace Workshop\Console;

use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use Workshop\Kafka\Client\AdminFactory;

#[AsCommand(
    name: 'kafka:topic:describe',
    description: 'Describe cluster topic.',
)]
final class TopicDescribeCommand extends Command
{
    public function __construct(
        private readonly AdminFactory $admin,
    ) {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this->addArgument('topic', InputArgument::REQUIRED, 'Topic to describe (e.g. enet.ecommerce.orders)');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $name = Input::string($input, 'topic');

        try {
            $info = $this->admin->create()->describe($name);
        } catch (\RuntimeException $e) {
            $output->writeln(sprintf('<error>%s</error>', $e->getMessage()));

            return Command::FAILURE;
        }

        $output->writeln(sprintf('<info>%s</info>', $info['name']));
        $output->writeln(sprintf('  partitions: %d', $info['partitions']));
        $output->writeln('  partition ids: ' . implode(', ', $info['partition_ids']));

        return Command::SUCCESS;
    }
}
