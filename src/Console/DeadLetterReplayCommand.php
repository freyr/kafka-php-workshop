<?php

declare(strict_types=1);

namespace Workshop\Console;

use Enqueue\RdKafka\RdKafkaMessage;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Workshop\Kernel\KafkaContextFactory;
use Workshop\Kernel\RetryRouter;

#[AsCommand(
    name: 'dlt:replay',
    description: 'Block 7: after the bug is fixed and deployed, re-publish Dead Letter Topic messages back to their original topic (from the x-original-topic header), original key preserved. --dry-run lists what would happen. Safe to re-run — the consumer dedups on event_id.',
)]
final class DeadLetterReplayCommand extends Command
{
    public function __construct(
        private readonly KafkaContextFactory $kafka,
    ) {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this
            ->addArgument('topic', InputArgument::OPTIONAL, 'Dead Letter Topic to drain', RetryRouter::DLT_TOPIC)
            ->addOption('dry-run', null, InputOption::VALUE_NONE, 'List what would be replayed without publishing anything')
            ->addOption('timeout', 't', InputOption::VALUE_REQUIRED, 'Receive timeout in ms (stop after this much silence)', 4000);
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $topic = (string) $input->getArgument('topic');
        $dryRun = (bool) $input->getOption('dry-run');
        $timeoutMs = (int) $input->getOption('timeout');

        // Fresh group so we read the whole DLT from the start each replay run.
        $readContext = $this->kafka->forConsumer('dlt-replay-' . uniqid());
        $consumer = $readContext->createConsumer($readContext->createTopic($topic));

        $produceContext = $this->kafka->forProducer();
        $producer = $produceContext->createProducer();

        $output->writeln(sprintf('<comment>%sreplaying %s</comment>', $dryRun ? '[DRY RUN] ' : '', $topic));

        $count = 0;
        $skipped = 0;
        while (true) {
            $message = $consumer->receive($timeoutMs);
            if (null === $message) {
                break;
            }
            if (! $message instanceof RdKafkaMessage) {
                continue;
            }

            $originalTopic = (string) ($message->getHeader('x-original-topic') ?? '');
            if ('' === $originalTopic) {
                ++$skipped;
                $output->writeln('  <error>✗ skip: no x-original-topic header</error>');
                continue;
            }

            $key = $message->getKey();
            if ($dryRun) {
                $output->writeln(sprintf('  would replay key=%s → %s (reason=%s)', $key ?? '<none>', $originalTopic, $message->getHeader('x-dead-letter-reason') ?? '?'));
                ++$count;
                continue;
            }

            $replay = $produceContext->createMessage($message->getBody(), [], [
                'x-replayed-from-dlt' => $topic,
                'x-replayed-at' => (string) time(),
            ]);
            $replay->setKey($key);
            $producer->send($produceContext->createTopic($originalTopic), $replay);
            ++$count;
            $output->writeln(sprintf('  <info>✓ replayed</info> key=%s → %s', $key ?? '<none>', $originalTopic));
        }

        $produceContext->close(); // flush all replayed messages
        $readContext->close();

        $output->writeln('');
        $output->writeln(sprintf(
            '<info>%s %d</info> message(s)%s',
            $dryRun ? 'would replay' : 'replayed',
            $count,
            $skipped > 0 ? sprintf(' · skipped %d', $skipped) : '',
        ));
        if (! $dryRun) {
            $output->writeln('<comment>re-run your consumer (without --poison) to process them; idempotency stops duplicate side-effects.</comment>');
        }

        return Command::SUCCESS;
    }
}
