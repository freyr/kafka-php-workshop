<?php

declare(strict_types=1);

namespace Workshop\Console;

use Doctrine\DBAL\Connection;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Workshop\Kernel\AvroEventSerializer;
use Workshop\Kernel\Database;
use Workshop\Kernel\IdempotencyStore;
use Workshop\Kernel\KafkaContextFactory;
use Workshop\Kernel\SideEffectStore;

#[AsCommand(
    name: 'delivery:consume',
    description: 'Block 5 demo: an at-least-once consumer that applies a side-effect per message in a MySQL transaction. Use --crash-after to simulate a crash before commit, and --idempotent to dedup by event_id.',
)]
final class DeliveryConsumeCommand extends Command
{
    public function __construct(
        private readonly KafkaContextFactory $kafka,
        private readonly AvroEventSerializer $avro,
        private readonly Database $db,
        private readonly IdempotencyStore $idempotency,
        private readonly SideEffectStore $sideEffects,
    ) {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this
            ->addArgument('topic', InputArgument::OPTIONAL, 'Topic to consume', 'enet.ecommerce.orders')
            ->addOption('group', 'g', InputOption::VALUE_REQUIRED, 'Consumer group (kept stable so offsets persist across runs)', 'delivery-demo')
            ->addOption('idempotent', null, InputOption::VALUE_NONE, 'Record event_id in the same transaction and skip already-applied events')
            ->addOption('crash-after', null, InputOption::VALUE_REQUIRED, 'Apply this many side-effects then exit WITHOUT committing the Kafka offset — simulates a crash before commit', 0)
            ->addOption('max', 'm', InputOption::VALUE_REQUIRED, 'Stop after receiving this many messages (0 = until the receive timeout)', 0)
            ->addOption('timeout', 't', InputOption::VALUE_REQUIRED, 'Receive timeout in ms', 5000);
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $topic = (string) $input->getArgument('topic');
        $group = (string) $input->getOption('group');
        $idempotent = (bool) $input->getOption('idempotent');
        $crashAfter = (int) $input->getOption('crash-after');
        $max = (int) $input->getOption('max');
        $timeoutMs = (int) $input->getOption('timeout');

        // In crash mode we never commit the Kafka offset — the whole batch stays
        // uncommitted so the recovery run redelivers it. The MySQL transaction,
        // however, always commits: that decoupling is the whole lesson.
        $commit = 0 === $crashAfter;

        $this->db->ensureSchema();

        $output->writeln(sprintf(
            '<comment>group=%s mode=%s%s</comment>',
            $group,
            $idempotent ? 'idempotent' : 'naive',
            $crashAfter > 0 ? sprintf(' crash-after=%d', $crashAfter) : '',
        ));

        $context = $this->kafka->forConsumer($group);
        $consumer = $context->createConsumer($context->createTopic($topic));
        $consumer->setCommitAsync(false);

        $applied = 0;
        $skipped = 0;
        $received = 0;

        while (0 === $max || $received < $max) {
            $message = $consumer->receive($timeoutMs);
            if (null === $message) {
                break;
            }
            ++$received;

            $event = $this->avro->decode($message->getBody());
            $metadata = $event['metadata'] ?? [];
            $eventId = (string) ($metadata['event_id'] ?? '');
            $eventType = (string) ($metadata['event_type'] ?? 'unknown');
            $orderId = (string) ($metadata['aggregate_id'] ?? '?');

            // Idempotency record + side-effect commit atomically in one transaction.
            $didApply = $this->db->transactional(function (Connection $tx) use ($idempotent, $eventId, $eventType, $orderId): bool {
                if ($idempotent && '' !== $eventId && ! $this->idempotency->recordIfNew($tx, $eventId, $eventType)) {
                    return false; // already processed — skip, transaction is a no-op
                }

                $this->sideEffects->apply($tx, $orderId, $eventId);

                return true;
            });

            if ($didApply) {
                ++$applied;
                $output->writeln(sprintf('  <info>✓ APPLIED side-effect</info> order=%s event=%s', $orderId, $this->short($eventId)));
            } else {
                ++$skipped;
                $output->writeln(sprintf('  <comment>↩ skip duplicate</comment> order=%s event=%s', $orderId, $this->short($eventId)));
            }

            if ($didApply && $crashAfter > 0 && $applied >= $crashAfter) {
                $output->writeln(sprintf('<error>💥 simulated crash AFTER db commit, BEFORE kafka commit</error> — applied %d, kafka-committed 0', $applied));
                $context->close();
                $this->summary($output, $applied, $skipped);

                return Command::SUCCESS;
            }

            if ($commit) {
                $consumer->acknowledge($message);
            }
        }

        $context->close();
        $output->writeln(sprintf('<comment>kafka-committed %d offset(s)</comment>', $applied + $skipped));
        $this->summary($output, $applied, $skipped);

        return Command::SUCCESS;
    }

    private function summary(OutputInterface $output, int $applied, int $skipped): void
    {
        $conn = $this->db->connection();
        $output->writeln(sprintf(
            'applied=%d skipped=%d · side_effects rows=%d · processed_events rows=%d',
            $applied,
            $skipped,
            $this->sideEffects->count($conn),
            $this->idempotency->count($conn),
        ));
    }

    private function short(string $eventId): string
    {
        return '' === $eventId ? '<none>' : substr($eventId, 0, 8);
    }
}
