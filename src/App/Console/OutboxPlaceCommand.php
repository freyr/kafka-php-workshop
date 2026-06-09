<?php

declare(strict_types=1);

namespace Workshop\App\Console;

use Doctrine\DBAL\Exception\DriverException;
use FlixTech\SchemaRegistryApi\Exception\SchemaRegistryException;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Uid\Uuid;
use Workshop\App\Outbox\OutboxPlacer;
use Workshop\App\Outbox\PayloadFormat;
use Workshop\App\Outbox\SimulatedCrash;
use Workshop\App\Producer\MessageCatalog;

#[AsCommand(
    name: 'outbox:place',
    description: 'Simulate the business write: mutate the order and append its event to the outbox in ONE DB transaction — no Kafka involved.',
)]
final class OutboxPlaceCommand extends Command
{
    /**
     * Only the state-changing order events make sense here — placing means "the
     * business did something", so each placement must have a state write to pair
     * with the outbox append. The audit/evolution messages from the wider catalog
     * change nothing and stay on the direct AVRO path (kafka:produce:sample).
     */
    private const array STATE_CHANGING = ['order.created', 'order.updated', 'order.cancelled'];

    public function __construct(
        private readonly OutboxPlacer $placer,
        private readonly MessageCatalog $catalog,
    ) {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this
            ->addOption('count', 'c', InputOption::VALUE_REQUIRED, 'How many business writes to place; default: 1', '1')
            ->addOption('message-name', null, InputOption::VALUE_REQUIRED, 'Place only this event (order.created | order.updated | order.cancelled); omit to pick a random one per write')
            ->addOption('pool', null, InputOption::VALUE_REQUIRED, 'Size of the reusable order-id pool; default: 8', '8')
            ->addOption('interval', null, InputOption::VALUE_REQUIRED, 'Milliseconds to pause between writes; default: 10', '10')
            ->addOption('fail', null, InputOption::VALUE_NONE, 'Crash each transaction right before COMMIT — the rollback beat: afterwards neither the order row nor the outbox row exists')
            ->addOption('format', null, InputOption::VALUE_REQUIRED, 'Payload encoding: json (envelope as JSON text) | avro (Confluent-framed bytes against the registered schemas — must match outbox:setup --format)', 'json');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $count = Input::int($input, 'count');
        $pin = Input::stringOrNull($input, 'message-name');
        $poolSize = Input::int($input, 'pool');
        $intervalMs = Input::int($input, 'interval');
        $fail = (bool) $input->getOption('fail');

        try {
            $format = PayloadFormat::fromOption(Input::string($input, 'format'));
        } catch (\InvalidArgumentException $e) {
            $output->writeln('<error>' . $e->getMessage() . '</error>');

            return Command::INVALID;
        }

        if ($count < 1) {
            $output->writeln('<error>--count must be >= 1.</error>');

            return Command::INVALID;
        }
        if (null !== $pin && ! in_array($pin, self::STATE_CHANGING, true)) {
            $output->writeln(sprintf('<error>Unknown message name: %s</error>', $pin));
            $output->writeln('Available: ' . implode(', ', self::STATE_CHANGING));

            return Command::INVALID;
        }
        if ($poolSize < 1) {
            $output->writeln('<error>--pool must be >= 1.</error>');

            return Command::INVALID;
        }

        $names = null !== $pin ? [$pin] : self::STATE_CHANGING;
        $orderIds = $this->orderPool($poolSize);

        $placed = 0;
        $rolledBack = 0;

        for ($i = 0; $i < $count; ++$i) {
            $name = $names[array_rand($names)];
            $orderId = $orderIds[array_rand($orderIds)];
            $message = $this->catalog->build($name, $orderId);

            try {
                $this->placer->place($message, $fail, $format);
            } catch (SimulatedCrash) {
                ++$rolledBack;
                $output->writeln(sprintf('<comment>✗ rolled back</comment> %s order=%s — crashed before COMMIT, so the order write AND the outbox row vanished together', $name, $orderId));

                continue;
            } catch (SchemaRegistryException) {
                $output->writeln('<error>No schema registered for this event.</error>');
                $output->writeln('AVRO placements encode against the registry — register schemas first, then place again:');
                $output->writeln('  <comment>bin/console kafka:schema:register --all</comment>');

                return Command::FAILURE;
            } catch (DriverException $e) {
                // The likeliest cause: encoding does not match the payload column
                // (AVRO bytes into a JSON column, or the reverse).
                $output->writeln(sprintf('<error>%s</error>', $e->getMessage()));
                $output->writeln(sprintf('Is the outbox provisioned for this format? Re-provision with:'));
                $output->writeln(sprintf('  <comment>bin/console outbox:setup --fresh --format %s</comment>', $format->value));

                return Command::FAILURE;
            }

            ++$placed;
            $output->writeln(sprintf('placed <info>%s</info> order=%s → outbox id=%s (unpublished)', $name, $orderId, $message->eventId()));

            if ($intervalMs > 0 && $i < $count - 1) {
                usleep($intervalMs * 1000);
            }
        }

        if ($rolledBack > 0) {
            $output->writeln(sprintf('<info>done</info> — %d rolled back, %d placed: check MySQL, the rolled-back writes left no trace in either table', $rolledBack, $placed));

            return Command::SUCCESS;
        }

        $output->writeln(sprintf('<info>done</info> — placed %d event(s) in the outbox. Kafka was never contacted: publish them with a relay (outbox:relay, or the Debezium connector).', $placed));

        return Command::SUCCESS;
    }

    /**
     * A fixed pool of synthetic order ids, so several event types can land on the
     * same order across a multi-write run (mirrors kafka:produce:sample).
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
