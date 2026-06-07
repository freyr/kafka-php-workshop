<?php

declare(strict_types=1);

namespace Workshop\Console;

use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Workshop\Kafka\Client\ProducerFactory;
use Workshop\Kafka\Serde\AvroEnvelopeSerializer;
use Workshop\Kafka\Serde\AvroPayload;
use Workshop\Kernel\EventFactory;
use Workshop\Kernel\WorkshopEvent;

#[AsCommand(
    name: 'events:produce',
    description: 'Build an enveloped event, AVRO-encode it against Schema Registry, and produce it to its topic keyed by aggregate_id.',
)]
final class EventProduceCommand extends Command
{
    public function __construct(
        private readonly ProducerFactory $producers,
        private readonly EventFactory $events,
        private readonly AvroEnvelopeSerializer $avro,
    ) {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this
            ->addArgument('type', InputArgument::REQUIRED, 'order-created | order-updated | order-cancelled | payment-processed | inventory-reserved')
            ->addOption('order-id', null, InputOption::VALUE_REQUIRED, 'Order id / aggregate id (default: generated)')
            ->addOption('correlation-id', null, InputOption::VALUE_REQUIRED, 'Continue an existing workflow (default: generated)')
            ->addOption('causation-id', null, InputOption::VALUE_REQUIRED, 'event_id of the event that caused this one')
            ->addOption('status', null, InputOption::VALUE_REQUIRED, 'For payment-processed: SUCCEEDED (default) or FAILED; for order-updated: the new status (default PAID)')
            ->addOption('reason', null, InputOption::VALUE_REQUIRED, 'For order-cancelled: cancellation reason (default CUSTOMER_REQUEST)');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $type = WorkshopEvent::tryFrom((string) $input->getArgument('type'));
        if (null === $type) {
            $output->writeln('<error>Unknown event type. Use: order-created | order-updated | order-cancelled | payment-processed | inventory-reserved</error>');

            return Command::INVALID;
        }

        $opts = array_filter([
            'order_id' => $input->getOption('order-id'),
            'correlation_id' => $input->getOption('correlation-id'),
            'causation_id' => $input->getOption('causation-id'),
            'status' => $input->getOption('status'),
            'reason' => $input->getOption('reason'),
        ], static fn (mixed $v): bool => null !== $v);

        /** @var array<string, string> $opts */
        $record = $this->events->build($type, $opts);
        $metadata = $record['metadata'];

        $payload = new AvroPayload($type->subject(), $type->schemaJson(), $record);

        $producer = $this->producers->create('producer.idempotent', $this->avro);
        $producer->keyed($type->topic(), (string) $metadata['aggregate_id'], $payload);
        $producer->close();

        $output->writeln("produced <info>{$type->eventType()}</info> → {$type->topic()} ({$type->subject()})");
        $output->writeln("  event_id       = {$metadata['event_id']}");
        $output->writeln("  correlation_id = {$metadata['correlation_id']}");
        $output->writeln('  causation_id   = ' . ($metadata['causation_id'] ?? '<null>'));
        $output->writeln("  aggregate_id   = {$metadata['aggregate_id']} (message key)");

        if (WorkshopEvent::OrderCreated === $type) {
            $orderId = $metadata['aggregate_id'];
            $correlationId = $metadata['correlation_id'];
            $output->writeln('');
            $output->writeln('<comment>continue this order\'s lifecycle on the SAME topic (multiple event types, one topic):</comment>');
            $output->writeln(sprintf('  events:produce order-updated   --order-id %s --correlation-id %s --status PAID', $orderId, $correlationId));
            $output->writeln(sprintf('  events:produce order-cancelled --order-id %s --correlation-id %s --reason OUT_OF_STOCK', $orderId, $correlationId));
            $output->writeln(sprintf('  events:dispatch enet.ecommerce.orders -m 3        # consumer dispatches by event_type'));
            $output->writeln('<comment>or branch into payment:</comment>');
            $output->writeln(sprintf(
                '  events:produce payment-processed --order-id %s --correlation-id %s --causation-id %s',
                $orderId,
                $correlationId,
                $metadata['event_id'],
            ));
        }

        return Command::SUCCESS;
    }
}
