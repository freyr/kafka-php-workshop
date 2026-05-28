<?php

declare(strict_types=1);

namespace Workshop\Console;

use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Workshop\Kernel\KafkaContextFactory;
use Workshop\Kernel\Topics;

#[AsCommand(
    name: 'consumer-groups:consume',
    description: 'Consume the consumer-groups topic under a named group, committing offsets.',
)]
final class ConsumeAsConsumerGroupCommand extends Command
{
    public function __construct(private readonly KafkaContextFactory $kafka)
    {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this
            ->addArgument('group', InputArgument::REQUIRED, 'Consumer group ID (e.g. group-a)')
            ->addOption('max', 'm', InputOption::VALUE_REQUIRED, 'Max messages to read', 5)
            ->addOption('timeout', 't', InputOption::VALUE_REQUIRED, 'Receive timeout in ms', 5000);
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $group     = (string) $input->getArgument('group');
        $max       = (int) $input->getOption('max');
        $timeoutMs = (int) $input->getOption('timeout');

        $context  = $this->kafka->forConsumer($group);
        $consumer = $context->createConsumer($context->createTopic(Topics::ConsumerGroups->value));
        $consumer->setCommitAsync(false);

        $deadline = microtime(true) + ($timeoutMs / 1000);
        $received = 0;

        while ($received < $max) {
            $remainingMs = (int) max(1, ($deadline - microtime(true)) * 1000);
            $message = $consumer->receive($remainingMs);
            if ($message === null) {
                break;
            }

            $kafkaMessage = $message->getKafkaMessage();
            $output->writeln(sprintf(
                'partition=%d offset=%d value=%s',
                $kafkaMessage->partition,
                $kafkaMessage->offset,
                $message->getBody(),
            ));

            $consumer->acknowledge($message);
            $received++;
        }

        $context->close();

        return Command::SUCCESS;
    }
}
