<?php

declare(strict_types=1);

namespace Workshop\Console;

use Doctrine\DBAL\Connection;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Workshop\Kernel\AvroEventSerializer;
use Workshop\Kernel\Database;
use Workshop\Kernel\KafkaContextFactory;
use Workshop\Kernel\OutboxStore;
use Workshop\Kernel\WorkshopEvent;

#[AsCommand(
    name: 'outbox:relay',
    description: 'Block 6 demo: the polling relay. Reads unpublished outbox rows, AVRO-encodes each, publishes to its topic, then marks it published. Kafka-first then mark = at-least-once.',
)]
final class OutboxRelayCommand extends Command
{
    public function __construct(
        private readonly Database $db,
        private readonly OutboxStore $outbox,
        private readonly AvroEventSerializer $avro,
        private readonly KafkaContextFactory $kafka,
    ) {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this
            ->addOption('once', null, InputOption::VALUE_NONE, 'Process one batch and exit (demo-friendly); otherwise poll forever')
            ->addOption('batch-size', 'b', InputOption::VALUE_REQUIRED, 'Rows per batch', 100)
            ->addOption('poll-interval', 'p', InputOption::VALUE_REQUIRED, 'Seconds to wait when the outbox is empty', 1);
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $this->db->ensureOutboxSchema();

        $once = (bool) $input->getOption('once');
        $batchSize = max(1, (int) $input->getOption('batch-size'));
        $pollInterval = max(1, (int) $input->getOption('poll-interval'));

        $output->writeln($once ? 'relay: one batch…' : 'relay started (Ctrl+C to stop)…');

        $total = 0;
        do {
            $published = $this->processBatch($output, $batchSize);
            $total += $published;

            if (0 === $published) {
                if ($once) {
                    break;
                }
                sleep($pollInterval);
            }
        } while (! $once);

        $output->writeln("done — published <info>{$total}</info> event(s); outbox unpublished now: <info>{$this->outbox->unpublishedCount($this->db->connection())}</info>");

        return Command::SUCCESS;
    }

    private function processBatch(OutputInterface $output, int $batchSize): int
    {
        $rows = $this->outbox->fetchUnpublished($this->db->connection(), $batchSize);
        if ([] === $rows) {
            return 0;
        }

        // Publish, then flush (context close waits for broker acks), THEN mark
        // published. A crash before the mark re-publishes next run — the relay is
        // at-least-once, so consumers must still dedup (Block 5 / Block 9 Knot 3).
        $context = $this->kafka->forProducer();
        $producer = $context->createProducer();
        $topic = $context->createTopic(WorkshopEvent::OrderCreated->topic());

        $ids = [];
        foreach ($rows as $row) {
            /** @var array<string, mixed> $record */
            $record = json_decode($row['payload'], true, flags: JSON_THROW_ON_ERROR);
            $binary = $this->avro->encode(WorkshopEvent::OrderCreated->subject(), WorkshopEvent::OrderCreated->schemaJson(), $record);

            $message = $context->createMessage($binary);
            $message->setKey($row['aggregate_id']);
            $producer->send($topic, $message);

            $ids[] = $row['id'];
            $output->writeln("  → {$row['event_type']} {$row['aggregate_id']} (event_id {$row['id']})");
        }

        $context->close();

        $this->db->transactional(fn (Connection $tx) => $this->outbox->markPublished($tx, $ids));

        return count($ids);
    }
}
