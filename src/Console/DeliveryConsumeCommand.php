<?php

declare(strict_types=1);

namespace Workshop\Console;

use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Workshop\Kernel\AvroEventSerializer;
use Workshop\Kernel\IdempotencyStore;
use Workshop\Kernel\KafkaContextFactory;
use Workshop\Kernel\SideEffectLog;

#[AsCommand(
    name: 'delivery:consume',
    description: 'Block 5 demo: an at-least-once consumer that applies a side-effect per message. Use --crash-after to simulate a crash before commit, and --idempotent to dedup by event_id.',
)]
final class DeliveryConsumeCommand extends Command
{
    public function __construct(
        private readonly KafkaContextFactory $kafka,
        private readonly AvroEventSerializer $avro,
        private readonly IdempotencyStore $store,
        private readonly SideEffectLog $sideEffects,
    ) {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this
            ->addArgument('topic', InputArgument::OPTIONAL, 'Topic to consume', 'enet.ecommerce.orders')
            ->addOption('group', 'g', InputOption::VALUE_REQUIRED, 'Consumer group (kept stable so offsets persist across runs)', 'delivery-demo')
            ->addOption('idempotent', null, InputOption::VALUE_NONE, 'Dedup by event_id via the idempotency store (skip already-applied events)')
            ->addOption('crash-after', null, InputOption::VALUE_REQUIRED, 'Apply this many side-effects then exit WITHOUT committing — simulates a crash before commit', 0)
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

        // In crash mode we never commit — the whole batch stays uncommitted so the
        // recovery run redelivers all of it. In normal mode we commit after each
        // message (at-least-once: process first, then commit).
        $commit = 0 === $crashAfter;

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
            $aggregate = (string) ($metadata['aggregate_id'] ?? '?');

            if ($idempotent && '' !== $eventId && $this->store->has($eventId)) {
                ++$skipped;
                $output->writeln(sprintf('  <comment>↩ skip duplicate</comment> order=%s event=%s', $aggregate, $this->short($eventId)));
                if ($commit) {
                    $consumer->acknowledge($message);
                }

                continue;
            }

            // Apply the side-effect. In a real consumer this is the DB write /
            // email / charge — here, one appended line we can count.
            $this->sideEffects->append(sprintf('order=%s event=%s', $aggregate, $eventId));
            if ($idempotent && '' !== $eventId) {
                $this->store->remember($eventId);
            }
            ++$applied;
            $output->writeln(sprintf('  <info>✓ APPLIED side-effect</info> order=%s event=%s', $aggregate, $this->short($eventId)));

            if ($crashAfter > 0 && $applied >= $crashAfter) {
                $output->writeln(sprintf('<error>💥 simulated crash BEFORE commit</error> — applied %d, committed 0 (offsets not advanced)', $applied));
                $context->close();
                $this->summary($output, $applied, $skipped);

                return Command::SUCCESS;
            }

            if ($commit) {
                $consumer->acknowledge($message);
            }
        }

        $context->close();
        $output->writeln(sprintf('<comment>committed %d offset(s)</comment>', $applied + $skipped));
        $this->summary($output, $applied, $skipped);

        return Command::SUCCESS;
    }

    private function summary(OutputInterface $output, int $applied, int $skipped): void
    {
        $output->writeln(sprintf(
            'applied=%d skipped=%d · side-effect log now has %d line(s)',
            $applied,
            $skipped,
            $this->sideEffects->count(),
        ));
    }

    private function short(string $eventId): string
    {
        return '' === $eventId ? '<none>' : substr($eventId, 0, 8);
    }
}
