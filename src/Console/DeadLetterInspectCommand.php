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
use Workshop\Kernel\AvroEventSerializer;
use Workshop\Kernel\KafkaContextFactory;
use Workshop\Kernel\RetryRouter;

#[AsCommand(
    name: 'dlt:inspect',
    description: 'Block 7: read the Dead Letter Topic and print each message\'s error metadata (origin, error, retries, reason) — the monitoring view. The DLT message carries everything needed to diagnose without the original logs.',
)]
final class DeadLetterInspectCommand extends Command
{
    public function __construct(
        private readonly KafkaContextFactory $kafka,
        private readonly AvroEventSerializer $avro,
    ) {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this
            ->addArgument('topic', InputArgument::OPTIONAL, 'Dead Letter Topic', RetryRouter::DLT_TOPIC)
            ->addOption('timeout', 't', InputOption::VALUE_REQUIRED, 'Receive timeout in ms (stop after this much silence)', 4000);
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $topic = (string) $input->getArgument('topic');
        $timeoutMs = (int) $input->getOption('timeout');

        // Fresh group each run so we always read the DLT from the beginning —
        // inspection is a snapshot, not a position we want to persist.
        $context = $this->kafka->forConsumer('dlt-inspect-' . uniqid());
        $consumer = $context->createConsumer($context->createTopic($topic));

        $output->writeln(sprintf('<comment>inspecting %s</comment>', $topic));

        $count = 0;
        while (true) {
            $message = $consumer->receive($timeoutMs);
            if (null === $message) {
                break;
            }
            if (! $message instanceof RdKafkaMessage) {
                continue;
            }
            ++$count;
            $this->render($output, $message, $count);
        }

        $context->close();
        $output->writeln('');
        $output->writeln(sprintf('<info>%d</info> message(s) in %s', $count, $topic));

        return Command::SUCCESS;
    }

    private function render(OutputInterface $output, RdKafkaMessage $message, int $n): void
    {
        $h = $message->getHeaders();
        $output->writeln('');
        $output->writeln(sprintf(
            '<info>[%d]</info> reason=%s retries=%s key=%s',
            $n,
            $h['x-dead-letter-reason'] ?? '?',
            $h['x-retry-count'] ?? '0',
            $message->getKey() ?? '<none>',
        ));
        $output->writeln(sprintf(
            '    from %s/p%s@offset %s  (host %s)',
            $h['x-original-topic'] ?? '?',
            $h['x-original-partition'] ?? '?',
            $h['x-original-offset'] ?? '?',
            $h['x-consumer-host'] ?? '?',
        ));
        $output->writeln(sprintf(
            '    error: %s: %s',
            $h['x-error-class'] ?? '?',
            $h['x-error-message'] ?? '?',
        ));

        $output->writeln('    payload: ' . $this->payloadSummary($message->getBody()));
    }

    private function payloadSummary(string $body): string
    {
        try {
            $event = $this->avro->decode($body);
            $metadata = $event['metadata'] ?? [];

            return sprintf(
                'order=%s event_type=%s event_id=%s',
                (string) ($metadata['aggregate_id'] ?? '?'),
                (string) ($metadata['event_type'] ?? '?'),
                substr((string) ($metadata['event_id'] ?? ''), 0, 8),
            );
        } catch (\Throwable) {
            return sprintf('<comment>(undecodable, %d bytes)</comment>', strlen($body));
        }
    }
}
