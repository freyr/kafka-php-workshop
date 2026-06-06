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
use Workshop\Kernel\KafkaContextFactory;
use Workshop\Kernel\WorkshopEvent;

#[AsCommand(
    name: 'events:dispatch',
    description: 'Block 3/4: consume a topic that carries MULTIPLE event types and route each message to a per-type handler by event_type. The consumer side of multiple-event-types-per-topic (RecordNameStrategy) — e.g. order-created / order-updated / order-cancelled all on enet.ecommerce.orders.',
)]
final class EventDispatchCommand extends Command
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
            ->addArgument('topic', InputArgument::OPTIONAL, 'Topic to consume', 'enet.ecommerce.orders')
            ->addOption('group', 'g', InputOption::VALUE_REQUIRED, 'Consumer group; omit for an ephemeral group from earliest')
            ->addOption('max', 'm', InputOption::VALUE_REQUIRED, 'Stop after this many messages (0 = until the receive timeout)', 0)
            ->addOption('timeout', 't', InputOption::VALUE_REQUIRED, 'Receive timeout in ms', 5000);
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $topic = (string) $input->getArgument('topic');
        $max = (int) $input->getOption('max');
        $timeoutMs = (int) $input->getOption('timeout');
        $group = null !== $input->getOption('group')
            ? (string) $input->getOption('group')
            : sprintf('dispatch-%s-%d-%d', $topic, getmypid(), $timeoutMs);

        $output->writeln(sprintf('<comment>dispatching %s · group=%s — routing by event_type</comment>', $topic, $group));

        $context = $this->kafka->forConsumer($group);
        $consumer = $context->createConsumer($context->createTopic($topic));
        $consumer->setCommitAsync(false);

        /** @var array<string, int> $tally */
        $tally = [
            'created' => 0,
            'updated' => 0,
            'cancelled' => 0,
            'other' => 0,
            'skipped' => 0,
        ];
        $received = 0;

        while (0 === $max || $received < $max) {
            $message = $consumer->receive($timeoutMs);
            if (null === $message) {
                break;
            }
            ++$received;

            // A consumer of a shared topic must tolerate bytes it cannot decode
            // (non-AVRO history, a schema it lacks). Skip, don't crash.
            try {
                $event = $this->avro->decode($message->getBody());
            } catch (\Throwable) {
                ++$tally['skipped'];
                $consumer->acknowledge($message);
                continue;
            }

            $metadata = $event['metadata'] ?? [];
            $eventType = (string) ($metadata['event_type'] ?? '');
            $orderId = (string) ($event['order_id'] ?? ($metadata['aggregate_id'] ?? '?'));

            // Dispatch by type — the heart of a multi-type-topic consumer.
            match (true) {
                $this->isType($eventType, WorkshopEvent::OrderCreated) => $this->onCreated($output, $orderId, $event, $tally),
                $this->isType($eventType, WorkshopEvent::OrderUpdated) => $this->onUpdated($output, $orderId, $event, $tally),
                $this->isType($eventType, WorkshopEvent::OrderCancelled) => $this->onCancelled($output, $orderId, $event, $tally),
                default => $this->onUnhandled($output, $eventType, $tally),
            };

            $consumer->acknowledge($message);
        }

        $context->close();

        $output->writeln('');
        $output->writeln(sprintf(
            'dispatched: created=%d updated=%d cancelled=%d · unhandled=%d skipped(non-AVRO)=%d',
            $tally['created'],
            $tally['updated'],
            $tally['cancelled'],
            $tally['other'],
            $tally['skipped'],
        ));

        return Command::SUCCESS;
    }

    private function isType(string $eventType, WorkshopEvent $candidate): bool
    {
        return $eventType === $candidate->eventType();
    }

    /**
     * @param array<string, mixed> $event
     * @param array<string, int>   $tally
     */
    private function onCreated(OutputInterface $output, string $orderId, array $event, array &$tally): void
    {
        $customer = $event['customer']['display_name'] ?? '?';
        $total = $event['totals']['total']['amount_cents'] ?? 0;
        $output->writeln(sprintf('  <info>▶ OPEN</info>   order=%s — new order for %s (total %d)', $orderId, $customer, $total));
        ++$tally['created'];
    }

    /**
     * @param array<string, mixed> $event
     * @param array<string, int>   $tally
     */
    private function onUpdated(OutputInterface $output, string $orderId, array $event, array &$tally): void
    {
        $from = $event['previous_status'] ?? '?';
        $to = $event['status'] ?? '?';
        $output->writeln(sprintf('  <comment>✎ UPDATE</comment> order=%s — status %s → %s', $orderId, $from, $to));
        ++$tally['updated'];
    }

    /**
     * @param array<string, mixed> $event
     * @param array<string, int>   $tally
     */
    private function onCancelled(OutputInterface $output, string $orderId, array $event, array &$tally): void
    {
        $reason = $event['reason'] ?? '?';
        $output->writeln(sprintf('  <error>✗ CANCEL</error> order=%s — reason %s', $orderId, $reason));
        ++$tally['cancelled'];
    }

    /**
     * @param array<string, int> $tally
     */
    private function onUnhandled(OutputInterface $output, string $eventType, array &$tally): void
    {
        // Forward-compatibility: a consumer ignores types it does not handle
        // rather than failing — new event types can ship without breaking it.
        $output->writeln(sprintf('  <comment>… ignore</comment> unhandled type %s', '' === $eventType ? '<none>' : $eventType));
        ++$tally['other'];
    }
}
