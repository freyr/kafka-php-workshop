<?php

declare(strict_types=1);

namespace Workshop\Console;

use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Workshop\Consume\DtoRouting;
use Workshop\Consume\MessageDenormalizer;
use Workshop\Consume\OrderCancelledDto;
use Workshop\Consume\OrderCreatedDto;
use Workshop\Consume\OrderUpdatedDto;
use Workshop\Kafka\Client\ConsumerFactory;
use Workshop\Kafka\Runtime\CommitPolicy;
use Workshop\Kafka\Runtime\ConsumerRunner;
use Workshop\Kafka\Runtime\RunLimits;
use Workshop\Kafka\Serde\AvroSerializer;

#[AsCommand(
    name: 'events:dispatch',
    description: 'Block 3/4: consume a topic carrying MULTIPLE event types and route each message to a per-type handler by name, denormalizing the payload into a typed read-model DTO via the Symfony Serializer.',
)]
final class EventDispatchCommand extends Command
{
    public function __construct(
        private readonly ConsumerFactory $consumers,
        private readonly ConsumerRunner $runner,
        private readonly AvroSerializer $avro,
        private readonly DtoRouting $dtoRouting,
        private readonly MessageDenormalizer $denormalizer,
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
        $topic = Input::string($input, 'topic');
        $max = Input::int($input, 'max');
        $timeoutMs = Input::int($input, 'timeout');
        $groupOption = Input::stringOrNull($input, 'group');
        $named = null !== $groupOption;
        $group = $groupOption ?? sprintf('dispatch-%s-%d-%d', $topic, getmypid(), $timeoutMs);

        $output->writeln(sprintf('<comment>dispatching %s · group=%s — routing by name</comment>', $topic, $group));

        /** @var array<string, int> $tally */
        $tally = [
            'created' => 0,
            'updated' => 0,
            'cancelled' => 0,
            'other' => 0,
            'malformed' => 0,
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
            if (! is_array($event)) {
                ++$tally['skipped'];

                return;
            }

            $name = $this->nameOf($event);
            $dtoClass = '' === $name ? null : $this->dtoRouting->for($name);
            if (null === $dtoClass) {
                $this->onUnhandled($output, $name, $tally);

                return;
            }

            $payload = $event;
            unset($payload['metadata']);

            try {
                $dto = $this->denormalizer->denormalize($payload, $dtoClass);
            } catch (\Throwable $e) {
                // Routed to a DTO, but the payload does not fit it (partial schema
                // evolution, a replayed old message, a producer bug). Tolerate it —
                // a shared-topic consumer must not die on one bad record.
                $output->writeln(sprintf('  <error>! MALFORMED</error> %s: %s', $name, $e->getMessage()));
                ++$tally['malformed'];

                return;
            }

            match (true) {
                $dto instanceof OrderCreatedDto => $this->onCreated($output, $dto, $tally),
                $dto instanceof OrderUpdatedDto => $this->onUpdated($output, $dto, $tally),
                $dto instanceof OrderCancelledDto => $this->onCancelled($output, $dto, $tally),
                default => $this->onUnhandled($output, $name, $tally),
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
            'dispatched: created=%d updated=%d cancelled=%d · unhandled=%d malformed=%d skipped(non-AVRO)=%d',
            $tally['created'],
            $tally['updated'],
            $tally['cancelled'],
            $tally['other'],
            $tally['malformed'],
            $tally['skipped'],
        ));

        return Command::SUCCESS;
    }

    /**
     * @param array<string, mixed> $event
     */
    private function nameOf(array $event): string
    {
        $metadata = is_array($event['metadata'] ?? null) ? $event['metadata'] : [];
        $name = $metadata['name'] ?? null;

        return is_string($name) ? $name : '';
    }

    /**
     * @param array<string, int> $tally
     */
    private function onCreated(OutputInterface $output, OrderCreatedDto $dto, array &$tally): void
    {
        $output->writeln(sprintf(
            '  <info>▶ OPEN</info>   order=%s — new order for %s (total %d)',
            $dto->orderId,
            '' !== $dto->customer->displayName ? $dto->customer->displayName : '?',
            $dto->totals->total->amountCents,
        ));
        ++$tally['created'];
    }

    /**
     * @param array<string, int> $tally
     */
    private function onUpdated(OutputInterface $output, OrderUpdatedDto $dto, array &$tally): void
    {
        $output->writeln(sprintf(
            '  <comment>✎ UPDATE</comment> order=%s — status %s → %s',
            $dto->orderId,
            $dto->previousStatus ?? '?',
            '' !== $dto->status ? $dto->status : '?',
        ));
        ++$tally['updated'];
    }

    /**
     * @param array<string, int> $tally
     */
    private function onCancelled(OutputInterface $output, OrderCancelledDto $dto, array &$tally): void
    {
        $output->writeln(sprintf(
            '  <error>✗ CANCEL</error> order=%s — reason %s',
            $dto->orderId,
            '' !== $dto->reason ? $dto->reason : '?',
        ));
        ++$tally['cancelled'];
    }

    /**
     * @param array<string, int> $tally
     */
    private function onUnhandled(OutputInterface $output, string $name, array &$tally): void
    {
        // Forward-compatibility: ignore types we do not consume rather than fail.
        $output->writeln(sprintf('  <comment>… ignore</comment> unhandled message %s', '' === $name ? '<none>' : $name));
        ++$tally['other'];
    }
}
