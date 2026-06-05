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
use Workshop\Kernel\EventFactory;
use Workshop\Kernel\KafkaContextFactory;
use Workshop\Kernel\OutboxStore;
use Workshop\Kernel\WorkshopEvent;

#[AsCommand(
    name: 'outbox:place',
    description: 'Block 6 demo: place an order. Default writes the order + an OrderCreated event to the outbox in ONE transaction. --naive publishes to Kafka directly (the dual-write trap).',
)]
final class OutboxPlaceCommand extends Command
{
    public function __construct(
        private readonly Database $db,
        private readonly OutboxStore $outbox,
        private readonly EventFactory $events,
        private readonly AvroEventSerializer $avro,
        private readonly KafkaContextFactory $kafka,
    ) {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this
            ->addOption('order-id', null, InputOption::VALUE_REQUIRED, 'Order / aggregate id (default: generated)')
            ->addOption('naive', null, InputOption::VALUE_NONE, 'Dual-write mode: commit the order, then publish to Kafka directly (no outbox)')
            ->addOption('crash', null, InputOption::VALUE_NONE, 'Simulate a crash right after the DB commit, before the next step runs');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $this->db->ensureOutboxSchema();

        $naive = (bool) $input->getOption('naive');
        $crash = (bool) $input->getOption('crash');

        $opts = array_filter([
            'order_id' => $input->getOption('order-id'),
        ], static fn (mixed $v): bool => null !== $v);

        /** @var array<string, string> $opts */
        $record = $this->events->build(WorkshopEvent::OrderCreated, $opts);
        $metadata = $record['metadata'];
        $orderId = (string) $metadata['aggregate_id'];
        $eventId = (string) $metadata['event_id'];

        return $naive
            ? $this->placeNaive($output, $record, $orderId, $eventId, $crash)
            : $this->placeViaOutbox($output, $record, $orderId, $eventId, $crash);
    }

    /**
     * The trap: two independent writes (DB, then Kafka). A crash between them
     * leaves the order persisted but the event lost — no downstream service ever
     * learns the order exists.
     *
     * @param array<string, mixed> $record
     */
    private function placeNaive(OutputInterface $output, array $record, string $orderId, string $eventId, bool $crash): int
    {
        $this->db->transactional(fn (Connection $tx): int => $this->insertOrder($tx, $record, $orderId));
        $output->writeln("<info>order committed</info> {$orderId} (naive / dual-write)");

        if ($crash) {
            $output->writeln('💥 simulated crash AFTER db commit, BEFORE kafka publish');
            $output->writeln('<comment>the order exists, but no OrderCreated event was ever published — silently inconsistent.</comment>');

            return Command::SUCCESS;
        }

        $binary = $this->avro->encode(WorkshopEvent::OrderCreated->subject(), WorkshopEvent::OrderCreated->schemaJson(), $record);
        $context = $this->kafka->forProducer();
        $message = $context->createMessage($binary);
        $message->setKey($orderId);
        $context->createProducer()->send($context->createTopic(WorkshopEvent::OrderCreated->topic()), $message);
        $context->close();

        $output->writeln('published <info>OrderCreated</info> → ' . WorkshopEvent::OrderCreated->topic() . " (event_id {$eventId})");
        $output->writeln('<comment>works only because nothing failed between the two writes — the window is the whole point.</comment>');

        return Command::SUCCESS;
    }

    /**
     * The fix: the order row and the OrderCreated event are one atomic write. A
     * crash after commit is harmless — the relay (outbox:relay or Debezium) will
     * publish the event.
     *
     * @param array<string, mixed> $record
     */
    private function placeViaOutbox(OutputInterface $output, array $record, string $orderId, string $eventId, bool $crash): int
    {
        $this->db->transactional(function (Connection $tx) use ($record, $orderId, $eventId): void {
            $this->insertOrder($tx, $record, $orderId);
            $this->outbox->add(
                $tx,
                $eventId,
                'Order',
                $orderId,
                (string) $record['metadata']['event_type'],
                (string) json_encode($record, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE),
            );
        });

        $output->writeln("<info>order + outbox row committed in ONE transaction</info> {$orderId} (event_id {$eventId})");

        if ($crash) {
            $output->writeln('💥 simulated crash AFTER the commit');
            $output->writeln('<comment>no loss: the event is durable in the outbox. A relay will publish it.</comment>');
        }

        $unpublished = $this->outbox->unpublishedCount($this->db->connection());
        $output->writeln('');
        $output->writeln("outbox unpublished rows: <info>{$unpublished}</info>");
        $output->writeln('<comment>publish them with:</comment>  bin/console outbox:relay --once');
        $output->writeln('<comment>or via CDC:</comment>          bin/debezium-register  (then consume enet.ecommerce.outbox.Order)');

        return Command::SUCCESS;
    }

    /**
     * @param array<string, mixed> $record
     */
    private function insertOrder(Connection $tx, array $record, string $orderId): int
    {
        /** @var array{customer_id: string} $customer */
        $customer = $record['customer'];
        /** @var array{total: array{amount_cents: int}} $totals */
        $totals = $record['totals'];

        return $tx->executeStatement(
            'INSERT INTO orders (order_id, customer_id, total_cents, created_at)
             VALUES (?, ?, ?, NOW(3))',
            [$orderId, $customer['customer_id'], $totals['total']['amount_cents']],
        );
    }
}
