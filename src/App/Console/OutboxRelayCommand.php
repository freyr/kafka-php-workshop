<?php

declare(strict_types=1);

namespace Workshop\App\Console;

use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Workshop\App\Outbox\OutboxRecord;
use Workshop\App\Outbox\OutboxRepository;
use Workshop\Kafka\Client\ProducerFactory;

#[AsCommand(
    name: 'outbox:relay',
    description: 'Poll the outbox table and publish pending rows to Kafka — the long-running PHP flavor of the Block 6 relay (Debezium CDC is the other).',
)]
final class OutboxRelayCommand extends Command
{
    public function __construct(
        private readonly OutboxRepository $outbox,
        private readonly ProducerFactory $producers,
    ) {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this
            ->addOption('batch', null, InputOption::VALUE_REQUIRED, 'Max rows fetched and published per poll; default: 100', '100')
            ->addOption('interval', null, InputOption::VALUE_REQUIRED, 'Milliseconds to sleep when a poll finds nothing pending; default: 500', '500')
            ->addOption('once', null, InputOption::VALUE_NONE, 'Drain the pending backlog and exit at the first empty poll, instead of polling until interrupted (Ctrl+C / SIGTERM)')
            ->addOption('profile', null, InputOption::VALUE_REQUIRED, 'Reliability profile: idempotent (default) | simple — same semantics as kafka:produce:sample', 'idempotent')
            ->addOption('topic-prefix', null, InputOption::VALUE_REQUIRED, 'Destination prefix the row\'s aggregate_type is appended to. Keep it equal to the Debezium route.topic.replacement so both relay flavors land in the same topics', 'enet.ecommerce.outbox.');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $batch = Input::int($input, 'batch');
        $intervalMs = Input::int($input, 'interval');
        $once = (bool) $input->getOption('once');
        $prefix = Input::string($input, 'topic-prefix');

        $profile = match (Input::string($input, 'profile')) {
            'idempotent' => 'producer.idempotent',
            'simple' => 'producer.simple',
            default => null,
        };
        if (null === $profile) {
            $output->writeln(sprintf('<error>Unknown profile: %s</error> (use: idempotent | simple)', Input::string($input, 'profile')));

            return Command::INVALID;
        }
        if ($batch < 1) {
            $output->writeln('<error>--batch must be >= 1.</error>');

            return Command::INVALID;
        }

        $narrate = $output->isVerbose() ? static fn (string $line) => $output->writeln($line) : null;
        $producer = $this->producers->createRaw($profile, $narrate);

        $output->writeln(sprintf(
            '<comment>relay started — %d event(s) pending, profile=%s, topics=%s*%s</comment>',
            $this->outbox->countUnpublished(),
            $profile,
            $prefix,
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

            $producer->resetDeliveryTally();
            foreach ($records as $record) {
                // Same header convention as the AVRO path: a consumer can route on
                // message-name and dedup on event-id without decoding the JSON body.
                $producer->produce(
                    $prefix . $record->aggregateType,
                    $record->aggregateId,
                    $record->payload,
                    [
                        'message-name' => $record->eventType,
                        'event-id' => $record->id,
                    ],
                );
            }
            $producer->flush();

            // Mark-after-ack: rows become published only once the broker confirmed
            // every record in the batch. A crash (or failure) between flush and the
            // UPDATE re-publishes the batch on the next run — at-least-once, the
            // consumer's event-id dedup (Block 5) absorbs the duplicates.
            if ($producer->failedDeliveries() > 0) {
                $output->writeln(sprintf('<error>%d of %d deliveries failed — batch left unpublished, retrying next poll</error>', $producer->failedDeliveries(), count($records)));
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

            $output->writeln(sprintf('relayed <info>%d</info> event(s) → %s* (total %d)', count($records), $prefix, $published));
        }

        $output->writeln(sprintf('<info>done</info> — relayed %d event(s); pending now: %d', $published, $this->outbox->countUnpublished()));

        return Command::SUCCESS;
    }
}
