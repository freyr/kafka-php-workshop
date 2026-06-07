<?php

declare(strict_types=1);

namespace Workshop\Console;

use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use Workshop\Kafka\Client\AdminFactory;

#[AsCommand(
    name: 'topic:list',
    description: 'Block 2: list the cluster topics via the raw \\RdKafka metadata API. php-rdkafka can read metadata but cannot create/delete topics — provisioning stays in the bin/ shell scripts.',
)]
final class TopicListCommand extends Command
{
    public function __construct(
        private readonly AdminFactory $admin,
    ) {
        parent::__construct();
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $topics = $this->admin->create()->list();

        $output->writeln(sprintf('<info>%d topic(s)</info>', count($topics)));
        foreach ($topics as $name) {
            $output->writeln('  ' . $name);
        }

        return Command::SUCCESS;
    }
}
