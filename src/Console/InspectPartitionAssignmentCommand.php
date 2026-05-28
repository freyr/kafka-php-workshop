<?php

declare(strict_types=1);

namespace Workshop\Console;

use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Workshop\Kernel\KafkaContextFactory;
use Workshop\Kernel\Topics;

#[AsCommand(
    name: 'partitioning:inspect',
    description: 'Read every record on the partitioning topic from earliest, printing key, partition and offset.',
)]
final class InspectPartitionAssignmentCommand extends Command
{
    public function __construct(
        private readonly KafkaContextFactory $kafka,
    ) {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this->addOption('timeout', 't', InputOption::VALUE_REQUIRED, 'Receive timeout in ms (exits on first timeout)', 5000);
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $timeoutMs = (int) $input->getOption('timeout');
        // Throwaway group: each run gets a unique id so we always start from the earliest offset.
        $group = sprintf('partitioning-inspect-%d-%d', getmypid(), time());

        $context = $this->kafka->forConsumer($group);
        $consumer = $context->createConsumer($context->createTopic(Topics::Partitioning->value));
        $consumer->setCommitAsync(false);

        while (true) {
            $message = $consumer->receive($timeoutMs);
            if (null === $message) {
                break;
            }

            $kafkaMessage = $message->getKafkaMessage();
            $output->writeln(sprintf(
                'partition=%d offset=%d key=%s value=%s',
                $kafkaMessage->partition,
                $kafkaMessage->offset,
                $message->getKey() ?? '<null>',
                $message->getBody(),
            ));
        }

        $context->close();

        return Command::SUCCESS;
    }
}
