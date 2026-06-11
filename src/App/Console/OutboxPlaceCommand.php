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
use Workshop\App\Producer\ErrorDemo;
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

    /**
     * The Block 7 demo event. Placeable only when pinned explicitly — the random
     * pick stays order-events-only, so the error demo can never leak into the
     * order topics (and vice versa: --poison is refused for anything else).
     */
    private const string ERROR_DEMO = 'error.demo';

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
            ->addOption('format', null, InputOption::VALUE_REQUIRED, 'Payload encoding: json (envelope as JSON text) | avro (Confluent-framed bytes against the registered schemas — must match outbox:setup --format)', 'json')
            ->addOption('poison', null, InputOption::VALUE_REQUIRED, 'Block 7: corrupt this many randomly-chosen placements (of --count) into poison — headers stay valid, the body becomes undecodable. Only with --message-name error.demo and --format avro', '0');
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

        $poison = Input::int($input, 'poison');

        if ($count < 1) {
            $output->writeln('<error>--count must be >= 1.</error>');

            return Command::INVALID;
        }
        if (null !== $pin && ! in_array($pin, [...self::STATE_CHANGING, self::ERROR_DEMO], true)) {
            $output->writeln(sprintf('<error>Unknown message name: %s</error>', $pin));
            $output->writeln('Available: ' . implode(', ', [...self::STATE_CHANGING, self::ERROR_DEMO]));

            return Command::INVALID;
        }
        if ($poolSize < 1) {
            $output->writeln('<error>--pool must be >= 1.</error>');

            return Command::INVALID;
        }
        if ($poison < 0 || $poison > $count) {
            $output->writeln('<error>--poison must be between 0 and --count.</error>');

            return Command::INVALID;
        }
        // Containment: poison can only ever land in the dedicated error-demo
        // topic family, and only as AVRO — the JSON outbox column would reject
        // corrupted bytes at INSERT, a confusing failure instead of a demo.
        if ($poison > 0 && self::ERROR_DEMO !== $pin) {
            $output->writeln('<error>--poison requires --message-name error.demo — injected poison must never land in the order topics.</error>');

            return Command::INVALID;
        }
        if ($poison > 0 && PayloadFormat::Avro !== $format) {
            $output->writeln('<error>--poison requires --format avro (provision with: outbox:setup --fresh --format avro).</error>');

            return Command::INVALID;
        }

        $names = null !== $pin ? [$pin] : self::STATE_CHANGING;
        $orderIds = $this->orderPool($poolSize);

        // Which of the $count placements get corrupted — chosen up front so the
        // poison is randomly distributed through the batch, not bunched at the end.
        $poisonAt = $this->poisonPositions($count, $poison);

        $placed = 0;
        $rolledBack = 0;
        $poisoned = 0;

        for ($i = 0; $i < $count; ++$i) {
            $name = $names[array_rand($names)];
            $isErrorDemo = self::ERROR_DEMO === $name;
            $message = $isErrorDemo
                ? ErrorDemo::create('err-' . substr(Uuid::v4()->toRfc4122(), 0, 8), $i + 1)
                : $this->catalog->build($name, $orderIds[array_rand($orderIds)]);
            $corrupt = isset($poisonAt[$i]);

            try {
                $this->placer->place($message, $fail, $format, $corrupt);
            } catch (SimulatedCrash) {
                ++$rolledBack;
                $output->writeln(sprintf('<comment>✗ rolled back</comment> %s key=%s — crashed before COMMIT, so the order write AND the outbox row vanished together', $name, $message->partitionKey()));

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
            if ($corrupt) {
                ++$poisoned;
                $output->writeln(sprintf('placed <fg=red>%s ☠ POISONED</> key=%s → outbox id=%s (unpublished) — headers valid, body corrupted', $name, $message->partitionKey(), $message->eventId()));
            } else {
                $output->writeln(sprintf('placed <info>%s</info> key=%s → outbox id=%s (unpublished)', $name, $message->partitionKey(), $message->eventId()));
            }

            if ($intervalMs > 0 && $i < $count - 1) {
                usleep($intervalMs * 1000);
            }
        }

        if ($rolledBack > 0) {
            $output->writeln(sprintf('<info>done</info> — %d rolled back, %d placed: check MySQL, the rolled-back writes left no trace in either table', $rolledBack, $placed));

            return Command::SUCCESS;
        }

        $output->writeln(sprintf(
            '<info>done</info> — placed %d event(s)%s in the outbox. Kafka was never contacted: publish them with a relay (outbox:relay, or the Debezium connector).',
            $placed,
            $poisoned > 0 ? sprintf(' (%d poisoned, randomly placed)', $poisoned) : '',
        ));

        return Command::SUCCESS;
    }

    /**
     * Pick which $poison of the $count placements get corrupted — a random,
     * order-preserving sample so the poison scatters through the batch.
     *
     * @return array<int, true> set of zero-based placement positions
     */
    private function poisonPositions(int $count, int $poison): array
    {
        if ($poison < 1) {
            return [];
        }

        $positions = (array) array_rand(array_fill(0, $count, true), $poison);

        return array_fill_keys(array_map(intval(...), $positions), true);
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
