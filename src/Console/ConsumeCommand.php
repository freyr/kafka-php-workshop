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

#[AsCommand(
    name: 'consume',
    description: 'Consume a topic under a consumer group, committing offsets. Omit --group to read from earliest under a throwaway group.',
)]
final class ConsumeCommand extends Command
{
    public function __construct(
        private readonly KafkaContextFactory $kafka,
    ) {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this
            ->addArgument('topic', InputArgument::REQUIRED, 'Topic to consume from (e.g. consumer-groups-events)')
            ->addOption('group', 'g', InputOption::VALUE_REQUIRED, 'Consumer group ID; omit for an ephemeral group that always starts from earliest')
            ->addOption('max', 'm', InputOption::VALUE_REQUIRED, 'Stop after this many messages (0 = read until the receive timeout)', 0)
            ->addOption('timeout', 't', InputOption::VALUE_REQUIRED, 'Receive timeout in ms; a poll returning nothing within it ends the run', 5000)
            ->addOption('no-commit', null, InputOption::VALUE_NONE, 'Do not commit offsets (read-only tail)');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $topicName = (string) $input->getArgument('topic');
        $max = (int) $input->getOption('max');
        $timeoutMs = (int) $input->getOption('timeout');
        $commit = ! (bool) $input->getOption('no-commit');
        $group = $this->resolveGroup($input->getOption('group'), $topicName);

        $output->writeln(sprintf('<comment>group=%s commit=%s</comment>', $group, $commit ? 'yes' : 'no'));

        $context = $this->kafka->forConsumer($group);
        $consumer = $context->createConsumer($context->createTopic($topicName));
        $consumer->setCommitAsync(false);

        $received = 0;
        while (0 === $max || $received < $max) {
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

            if ($commit) {
                $consumer->acknowledge($message);
            }
            ++$received;
        }

        $context->close();

        return Command::SUCCESS;
    }

    /**
     * A named group keeps its committed offsets across runs (real consumer-group
     * behaviour); an ephemeral, never-reused id forces every run to start at the
     * earliest offset — handy for inspecting a topic's full contents.
     */
    private function resolveGroup(mixed $group, string $topicName): string
    {
        if (null !== $group) {
            return (string) $group;
        }

        return sprintf('ephemeral-%s-%d-%d', $topicName, getmypid(), time());
    }
}
