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
use Workshop\Kernel\EventFactory;
use Workshop\Kernel\KafkaContextFactory;
use Workshop\Kernel\WorkshopEvent;

#[AsCommand(
    name: 'events:produce',
    description: 'Build an enveloped event, AVRO-encode it against Schema Registry, and produce it to its topic keyed by aggregate_id.',
)]
final class EventProduceCommand extends Command
{
    public function __construct(
        private readonly KafkaContextFactory $kafka,
        private readonly EventFactory $events,
        private readonly AvroEventSerializer $avro,
    ) {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this
            ->addArgument('type', InputArgument::REQUIRED, 'order-created | payment-processed | inventory-reserved')
            ->addOption('order-id', null, InputOption::VALUE_REQUIRED, 'Order id / aggregate id (default: generated)')
            ->addOption('correlation-id', null, InputOption::VALUE_REQUIRED, 'Continue an existing workflow (default: generated)')
            ->addOption('causation-id', null, InputOption::VALUE_REQUIRED, 'event_id of the event that caused this one')
            ->addOption('status', null, InputOption::VALUE_REQUIRED, 'For payment-processed: SUCCEEDED (default) or FAILED');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $type = WorkshopEvent::tryFrom((string) $input->getArgument('type'));
        if (null === $type) {
            $output->writeln('<error>Unknown event type. Use: order-created | payment-processed | inventory-reserved</error>');

            return Command::INVALID;
        }

        $opts = array_filter([
            'order_id' => $input->getOption('order-id'),
            'correlation_id' => $input->getOption('correlation-id'),
            'causation_id' => $input->getOption('causation-id'),
            'status' => $input->getOption('status'),
        ], static fn (mixed $v): bool => null !== $v);

        /** @var array<string, string> $opts */
        $record = $this->events->build($type, $opts);
        $metadata = $record['metadata'];
        $binary = $this->avro->encode($type->subject(), $type->schemaJson(), $record);

        $context = $this->kafka->forProducer();
        $message = $context->createMessage($binary);
        $message->setKey((string) $metadata['aggregate_id']);
        $context->createProducer()->send($context->createTopic($type->topic()), $message);
        $context->close();

        $output->writeln("produced <info>{$type->eventType()}</info> → {$type->topic()} ({$type->subject()})");
        $output->writeln("  event_id       = {$metadata['event_id']}");
        $output->writeln("  correlation_id = {$metadata['correlation_id']}");
        $output->writeln('  causation_id   = ' . ($metadata['causation_id'] ?? '<null>'));
        $output->writeln("  aggregate_id   = {$metadata['aggregate_id']} (message key)");

        if (WorkshopEvent::OrderCreated === $type) {
            $output->writeln('');
            $output->writeln('<comment>chain the next step:</comment>');
            $output->writeln(sprintf(
                '  events:produce payment-processed --order-id %s --correlation-id %s --causation-id %s',
                $metadata['aggregate_id'],
                $metadata['correlation_id'],
                $metadata['event_id'],
            ));
        }

        return Command::SUCCESS;
    }
}
