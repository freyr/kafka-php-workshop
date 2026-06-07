<?php

declare(strict_types=1);

namespace Workshop\Console;

use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Workshop\Kafka\Client\ConsumerFactory;
use Workshop\Kafka\Runtime\CommitPolicy;
use Workshop\Kafka\Runtime\ConsumerRunner;
use Workshop\Kafka\Runtime\RunLimits;
use Workshop\Kafka\Serde\AvroEnvelopeSerializer;
use Workshop\Kernel\WorkshopEvent;

#[AsCommand(
    name: 'events:dispatch',
    description: 'Block 3/4: consume a topic that carries MULTIPLE event types and route each message to a per-type handler by event_type. The consumer side of multiple-event-types-per-topic (RecordNameStrategy) — e.g. order-created / order-updated / order-cancelled all on enet.ecommerce.orders.',
)]
final class EventDispatchCommand extends Command
{
    use EventEnvelope;
    use InputCasts;

    public function __construct(
        private readonly ConsumerFactory $consumers,
        private readonly ConsumerRunner $runner,
        private readonly AvroEnvelopeSerializer $avro,
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
        $topic = $this->argString($input, 'topic');
        $max = $this->optInt($input, 'max');
        $timeoutMs = $this->optInt($input, 'timeout');
        $groupOption = $this->optString($input, 'group');
        $named = null !== $groupOption;
        $group = $groupOption ?? sprintf('dispatch-%s-%d-%d', $topic, getmypid(), $timeoutMs);

        $output->writeln(sprintf('<comment>dispatching %s · group=%s — routing by event_type</comment>', $topic, $group));

        /** @var array<string, int> $tally */
        $tally = [
            'created' => 0,
            'updated' => 0,
            'cancelled' => 0,
            'other' => 0,
            'skipped' => 0,
        ];

        $handler = function (\RdKafka\Message $message) use ($output, &$tally): void {
            // A consumer of a shared topic must tolerate bytes it cannot decode
            // (non-AVRO history, a schema it lacks). Skip, don't crash.
            try {
                $event = $this->avro->decode($message->payload);
            } catch (\Throwable) {
                $event = null;
            }
            if (null === $event) {
                ++$tally['skipped'];

                return;
            }

            $metadata = $this->metadataOf($event);
            $eventType = $this->string($metadata, 'event_type');
            $orderId = $this->digString($event, 'order_id');
            if ('' === $orderId) {
                $orderId = $this->string($metadata, 'aggregate_id');
            }
            if ('' === $orderId) {
                $orderId = '?';
            }

            // Dispatch by type — the heart of a multi-type-topic consumer.
            match (true) {
                $this->isType($eventType, WorkshopEvent::OrderCreated) => $this->onCreated($output, $orderId, $event, $tally),
                $this->isType($eventType, WorkshopEvent::OrderUpdated) => $this->onUpdated($output, $orderId, $event, $tally),
                $this->isType($eventType, WorkshopEvent::OrderCancelled) => $this->onCancelled($output, $orderId, $event, $tally),
                default => $this->onUnhandled($output, $eventType, $tally),
            };
        };

        $this->runner->run(
            $this->consumers->create($named ? 'consumer.at-least-once' : 'consumer.ephemeral', $group),
            [$topic],
            $handler,
            new RunLimits(maxMessages: $max, pollTimeoutMs: $timeoutMs, stopOnIdle: true),
            CommitPolicy::AfterEachMessage,
        );

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
        $customer = $this->digString($event, 'customer', 'display_name');
        $total = $this->digInt($event, 'totals', 'total', 'amount_cents');
        $output->writeln(sprintf('  <info>▶ OPEN</info>   order=%s — new order for %s (total %d)', $orderId, '' !== $customer ? $customer : '?', $total));
        ++$tally['created'];
    }

    /**
     * @param array<string, mixed> $event
     * @param array<string, int>   $tally
     */
    private function onUpdated(OutputInterface $output, string $orderId, array $event, array &$tally): void
    {
        $from = $this->digString($event, 'previous_status') ?: '?';
        $to = $this->digString($event, 'status') ?: '?';
        $output->writeln(sprintf('  <comment>✎ UPDATE</comment> order=%s — status %s → %s', $orderId, $from, $to));
        ++$tally['updated'];
    }

    /**
     * @param array<string, mixed> $event
     * @param array<string, int>   $tally
     */
    private function onCancelled(OutputInterface $output, string $orderId, array $event, array &$tally): void
    {
        $reason = $this->digString($event, 'reason') ?: '?';
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
