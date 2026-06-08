<?php

declare(strict_types=1);

namespace Workshop\Console;

use FlixTech\SchemaRegistryApi\Exception\SchemaRegistryException;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Uid\Uuid;
use Workshop\Kafka\Client\ProducerFactory;
use Workshop\Kafka\Serde\MessageSerializer;
use Workshop\Produce\MessageCatalog;

#[AsCommand(
    name: 'produce',
    description: 'Stream AVRO events to their routed topics. Each message is picked at random from the catalog (pin one with --message-name) and keyed by an order id drawn from a small reusable pool. With --count it sends that many and stops; without it, it streams until Ctrl+C.',
)]
final class ProduceCommand extends Command
{
    public function __construct(
        private readonly ProducerFactory $producers,
        private readonly MessageSerializer $serializer,
        private readonly MessageCatalog $catalog,
    ) {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this
            ->addOption('count', 'c', InputOption::VALUE_REQUIRED, 'How many messages to produce, then stop. Omit to stream indefinitely until interrupted (Ctrl+C / SIGTERM).')
            ->addOption('message-name', null, InputOption::VALUE_REQUIRED, 'Produce only this message (e.g. order.created); omit to pick a random message per send')
            ->addOption('pool', null, InputOption::VALUE_REQUIRED, 'Size of the reusable order-id pool keys are drawn from; a smaller pool puts more event types on the same order (same key → same partition, ordered)', '8')
            ->addOption('interval', null, InputOption::VALUE_REQUIRED, 'Milliseconds to pause between sends; useful for watching a consumer keep up with an indefinite stream', '0');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $max = Input::intOrNull($input, 'count');
        $pin = Input::stringOrNull($input, 'message-name');
        $poolSize = Input::int($input, 'pool');
        $intervalMs = Input::int($input, 'interval');

        if (null !== $pin && ! $this->catalog->has($pin)) {
            $output->writeln(sprintf('<error>Unknown message name: %s</error>', $pin));
            $output->writeln('Available: ' . implode(', ', $this->catalog->names()));

            return Command::INVALID;
        }
        if (null !== $max && $max < 1) {
            $output->writeln('<error>--count must be >= 1 (omit it entirely to stream indefinitely).</error>');

            return Command::INVALID;
        }
        if ($poolSize < 1) {
            $output->writeln('<error>--pool must be >= 1.</error>');

            return Command::INVALID;
        }

        $names = null !== $pin ? [$pin] : $this->catalog->names();
        $orderIds = $this->orderPool($poolSize);

        $producer = $this->producers->create('producer.idempotent', $this->serializer);

        $running = true;
        pcntl_async_signals(true);
        $stop = static function () use (&$running): void {
            $running = false;
        };
        pcntl_signal(SIGINT, $stop);
        pcntl_signal(SIGTERM, $stop);

        if (null === $max) {
            $output->writeln('<comment>streaming until interrupted (Ctrl+C) — flushing on exit…</comment>');
        }

        $sent = 0;

        try {
            while ($running && (null === $max || $sent < $max)) {
                $name = $names[array_rand($names)];
                $orderId = $orderIds[array_rand($orderIds)];
                $message = $this->catalog->build($name, $orderId);

                $produced = $producer->produce($message);
                ++$sent;

                $output->writeln(sprintf('produced <info>%s</info> → %s key=%s', $produced->name, $produced->route->topic, $message->partitionKey()));

                // An async SIGINT/SIGTERM interrupts the sleep, so the loop
                // condition re-checks $running right after without waiting it out.
                if ($intervalMs > 0) {
                    usleep($intervalMs * 1000);
                }
            }
        } catch (SchemaRegistryException) {
            $output->writeln('<error>No schema registered for this event.</error>');
            $output->writeln('Schemas are not auto-registered — register them first, then produce again:');
            $output->writeln('  <comment>bin/console schema:register --all</comment>');

            return Command::FAILURE;
        } finally {
            $producer->close();
        }

        $output->writeln(sprintf('<info>done</info> — produced %d message(s)', $sent));

        return Command::SUCCESS;
    }

    /**
     * A fixed pool of synthetic order ids. Keys are drawn from it at random, so a
     * small pool concentrates several event types onto the same order (ordered
     * within its partition) while a large pool spreads them across partitions.
     *
     * @return list<string>
     */
    private function orderPool(int $size): array
    {
        return array_map(
            static fn (): string => 'ord-' . substr(Uuid::v4()->toRfc4122(), 0, 8),
            range(1, $size),
        );
    }
}
