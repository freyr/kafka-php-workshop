<?php

declare(strict_types=1);

namespace Workshop\Console;

use FlixTech\SchemaRegistryApi\Exception\SchemaRegistryException;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Uid\Uuid;
use Workshop\Kafka\Client\ProducerFactory;
use Workshop\Kafka\Serde\AvroPayload;
use Workshop\Kafka\Serde\AvroSerializer;
use Workshop\Produce\InventoryReserved;
use Workshop\Produce\Message;
use Workshop\Produce\MessageNameResolver;
use Workshop\Produce\MessageRouting;
use Workshop\Produce\OrderCancelled;
use Workshop\Produce\OrderCreated;
use Workshop\Produce\OrderUpdated;
use Workshop\Produce\PaymentProcessed;

#[AsCommand(
    name: 'events:produce',
    description: 'Build a typed Message, AVRO-encode its envelope against Schema Registry, and produce it to its routed topic keyed by partitionKey.',
)]
final class EventProduceCommand extends Command
{
    public function __construct(
        private readonly ProducerFactory $producers,
        private readonly MessageRouting $routing,
        private readonly AvroSerializer $avro,
        private readonly MessageNameResolver $names,
    ) {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this
            ->addArgument('type', InputArgument::REQUIRED, 'order-created | order-updated | order-cancelled | payment-processed | inventory-reserved')
            ->addOption('order-id', null, InputOption::VALUE_REQUIRED, 'Order id / partition key (default: generated)')
            ->addOption('status', null, InputOption::VALUE_REQUIRED, 'For payment-processed: SUCCEEDED (default) or FAILED; for order-updated: the new status (default PAID)')
            ->addOption('reason', null, InputOption::VALUE_REQUIRED, 'For order-cancelled: cancellation reason (default CUSTOMER_REQUEST)');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $type = Input::string($input, 'type');
        $orderId = Input::stringOrNull($input, 'order-id') ?? 'ord-' . substr(Uuid::v4()->toRfc4122(), 0, 8);
        $status = Input::stringOrNull($input, 'status');
        $reason = Input::stringOrNull($input, 'reason');

        $message = $this->message($type, $orderId, $status, $reason);
        if (null === $message) {
            $output->writeln('<error>Unknown event type. Use: order-created | order-updated | order-cancelled | payment-processed | inventory-reserved</error>');

            return Command::INVALID;
        }

        $name = $this->names->nameOf($message);
        $route = $this->routing->for($name);
        $payload = new AvroPayload($route->subject, $route->schemaJson(), $message->envelope($name));

        $producer = $this->producers->create('producer.idempotent', $this->avro);

        try {
            $producer->keyed($route->topic, $message->partitionKey(), $payload);
            $producer->close();
        } catch (SchemaRegistryException) {
            $output->writeln(sprintf('<error>No schema registered for subject %s.</error>', $route->subject));
            $output->writeln('Schemas are not auto-registered — register it first, then produce again:');
            $output->writeln(sprintf('  <comment>bin/console schema:register %s</comment>', $type));

            return Command::FAILURE;
        }

        $output->writeln(sprintf('produced <info>%s</info> → %s (%s)', $name, $route->topic, $route->subject));
        $output->writeln('  key = ' . $message->partitionKey() . ' (message key / partition key)');

        if ($message instanceof OrderCreated) {
            $output->writeln('');
            $output->writeln('<comment>continue this order\'s lifecycle on the SAME topic (multiple event types, one topic):</comment>');
            $output->writeln(sprintf('  events:produce order-updated   --order-id %s --status PAID', $orderId));
            $output->writeln(sprintf('  events:produce order-cancelled --order-id %s --reason OUT_OF_STOCK', $orderId));
            $output->writeln('  events:dispatch enet.ecommerce.orders -m 3        # consumer dispatches by name');
            $output->writeln('<comment>or branch into payment:</comment>');
            $output->writeln(sprintf('  events:produce payment-processed --order-id %s', $orderId));
        }

        return Command::SUCCESS;
    }

    private function message(string $type, string $orderId, ?string $status, ?string $reason): ?Message
    {
        return match ($type) {
            'order-created' => OrderCreated::create($orderId),
            'order-updated' => OrderUpdated::create($orderId, $status ?? 'PAID'),
            'order-cancelled' => OrderCancelled::create($orderId, $reason ?? 'CUSTOMER_REQUEST'),
            'payment-processed' => PaymentProcessed::create($orderId, $status ?? 'SUCCEEDED'),
            'inventory-reserved' => InventoryReserved::create($orderId),
            default => null,
        };
    }
}
