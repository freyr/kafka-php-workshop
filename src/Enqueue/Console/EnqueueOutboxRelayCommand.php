<?php

declare(strict_types=1);

namespace Workshop\Enqueue\Console;

use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Workshop\App\Console\Input;
use Workshop\App\Outbox\OutboxRecord;
use Workshop\App\Outbox\OutboxRepository;
use Workshop\Enqueue\EnqueueContextFactory;
use Workshop\Kafka\Callback\DeliveryTally;

#[AsCommand(
    name: 'enqueue:outbox:relay',
    description: 'Poll the outbox table and publish pending rows to Kafka through enqueue — the typical long-running relay daemon.',
)]
final class EnqueueOutboxRelayCommand extends Command
{
    /**
     * Destination prefix the row's aggregate_type is appended to — fixed to the
     * same topics the pure-rdkafka relay and the Debezium connector publish to, so
     * all three relay flavors are interchangeable on the wire.
     */
    private const string TOPIC_PREFIX = 'enet.ecommerce.outbox.';

    private const int FLUSH_TIMEOUT_MS = 10000;

    public function __construct(
        private readonly EnqueueContextFactory $contexts,
        private readonly OutboxRepository $outbox,
    ) {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this
            ->addOption('batch', null, InputOption::VALUE_REQUIRED, 'Max rows fetched and published per poll; default: 100', '100')
            ->addOption('interval', null, InputOption::VALUE_REQUIRED, 'Milliseconds to sleep when a poll finds nothing pending; default: 500', '500')
            ->addOption('once', null, InputOption::VALUE_NONE, 'Drain the pending backlog and exit at the first empty poll, instead of polling until interrupted (Ctrl+C / SIGTERM)');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $batch = Input::int($input, 'batch');
        $intervalMs = Input::int($input, 'interval');
        $once = (bool) $input->getOption('once');

        if ($batch < 1) {
            $output->writeln('<error>--batch must be >= 1.</error>');

            return Command::INVALID;
        }

        $narrate = $output->isVerbose() ? static fn (string $line) => $output->writeln($line) : null;

        // The tally is shared between the context's delivery-report callback and
        // this loop: flush() drains the queue, then failed() says whether every
        // record in the batch was actually broker-acked — the precondition for
        // marking rows published.
        $tally = new DeliveryTally($narrate);
        $context = $this->contexts->relayProducer($tally);
        $producer = $context->createProducer();

        $output->writeln(sprintf(
            '<comment>enqueue relay started — %d event(s) pending, idempotent producer, topics=%s*%s</comment>',
            $this->outbox->countUnpublished(),
            self::TOPIC_PREFIX,
            $once ? ', draining once' : ', polling until interrupted (Ctrl+C)',
        ));

        $running = true;
        pcntl_async_signals(true);
        $stop = static function () use (&$running): void {
            $running = false;
        };
        pcntl_signal(SIGINT, $stop);
        pcntl_signal(SIGTERM, $stop);

        $published = 0;

        while ($running) {
            $records = $this->outbox->fetchUnpublished($batch);

            if ([] === $records) {
                if ($once) {
                    break;
                }
                // An async SIGINT/SIGTERM interrupts the sleep, so the loop
                // condition re-checks $running right after without waiting it out.
                if ($intervalMs > 0) {
                    usleep($intervalMs * 1000);
                }

                continue;
            }

            $tally->reset();
            foreach ($records as $record) {
                // Same header convention as every other producer here: a consumer
                // routes on message-name and dedups on event-id without decoding.
                $message = $context->createMessage($record->payload, [], [
                    'message-name' => $record->eventType,
                    'event-id' => $record->id,
                ]);
                $message->setKey($record->aggregateId);

                $producer->send($context->createTopic(self::TOPIC_PREFIX . $record->aggregateType), $message);
            }

            $flushResult = $producer->flush(self::FLUSH_TIMEOUT_MS);
            $flushFailed = null !== $flushResult && RD_KAFKA_RESP_ERR_NO_ERROR !== $flushResult;

            // Mark-after-ack: rows become published only once the broker confirmed
            // every record in the batch. A crash (or failure) between flush and the
            // UPDATE re-publishes the batch on the next run — at-least-once, the
            // consumer's event-id dedup absorbs the duplicates.
            if ($flushFailed || $tally->failed() > 0) {
                $output->writeln(sprintf(
                    '<error>%s — batch left unpublished, retrying next poll</error>',
                    $flushFailed
                        ? sprintf('flush did not drain within %dms', self::FLUSH_TIMEOUT_MS)
                        : sprintf('%d of %d deliveries failed', $tally->failed(), count($records)),
                ));
                if ($once) {
                    return Command::FAILURE;
                }
                if ($intervalMs > 0) {
                    usleep($intervalMs * 1000);
                }

                continue;
            }

            $this->outbox->markPublished(array_map(static fn (OutboxRecord $record): int => $record->position, $records));
            $published += count($records);

            $output->writeln(sprintf('relayed <info>%d</info> event(s) → %s* (total %d)', count($records), self::TOPIC_PREFIX, $published));
        }

        $output->writeln(sprintf('<info>done</info> — relayed %d event(s); pending now: %d', $published, $this->outbox->countUnpublished()));

        return Command::SUCCESS;
    }
}
