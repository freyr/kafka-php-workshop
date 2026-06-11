<?php

declare(strict_types=1);

namespace Workshop\Enqueue\Console;

use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Workshop\App\Console\Input;
use Workshop\App\Consumer\ConsoleWriter;
use Workshop\App\Consumer\ConsumedMessage;
use Workshop\App\Consumer\MessageBus;
use Workshop\App\Consumer\MessageInterpreter;
use Workshop\App\Consumer\OrderEvent;
use Workshop\Enqueue\EnqueueContextFactory;

#[AsCommand(
    name: 'enqueue:consume',
    description: 'Consume a topic through enqueue into the orders projection — explicit commit after handling, dedup middleware always on (effectively-once).',
)]
final class EnqueueConsumeCommand extends Command
{
    /**
     * One poll slice. receive() returns null when the slice passes without a
     * record, which is also how often the loop re-checks its stop conditions
     * (signals, --max) — the consumer never blocks longer than this.
     */
    private const int POLL_MS = 1000;

    public function __construct(
        private readonly EnqueueContextFactory $contexts,
        private readonly MessageInterpreter $interpreter,
        private readonly MessageBus $bus,
        private readonly ConsoleWriter $console,
    ) {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this
            ->addArgument('topic', InputArgument::OPTIONAL, 'Topic to consume', 'enet.ecommerce.orders')
            ->addOption('group', null, InputOption::VALUE_REQUIRED, 'Consumer group id', 'enqueue-workshop')
            ->addOption('max', null, InputOption::VALUE_REQUIRED, 'Stop after this many records (0 = tail until Ctrl+C)', '0');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        // Hand the run's output to the console sink so bus handlers that print
        // have somewhere to write.
        $this->console->bind($output);

        $topic = Input::string($input, 'topic');
        $group = Input::string($input, 'group');
        $max = Input::int($input, 'max');

        if ($max < 0) {
            $output->writeln('<error>--max must be >= 0 (0 tails until interrupted).</error>');

            return Command::INVALID;
        }

        $context = $this->contexts->consumer($group);
        $consumer = $context->createConsumer($context->createTopic($topic));

        $output->writeln(sprintf(
            '<comment>topic=%s group=%s commit=explicit-after-handle dedup=on max=%s</comment>',
            $topic,
            $group,
            0 === $max ? '∞ (Ctrl+C to stop)' : (string) $max,
        ));

        $running = true;
        pcntl_async_signals(true);
        $stop = static function () use (&$running): void {
            $running = false;
        };
        pcntl_signal(SIGINT, $stop);
        pcntl_signal(SIGTERM, $stop);

        $handled = 0;
        $skipped = 0;

        while ($running && (0 === $max || $handled + $skipped < $max)) {
            $record = $consumer->receive(self::POLL_MS);
            if (null === $record) {
                continue; // idle slice — loop around and re-check the stop conditions
            }
            $kafkaMessage = $record->getKafkaMessage();
            if (null === $kafkaMessage) {
                continue; // enqueue contract: a received record always carries one
            }

            // The same pipeline as kafka:consume: decode + denormalize into a typed
            // DTO, then dispatch through the bus with the transaction + dedup
            // middleware folded around the handler — always idempotent here, the
            // production default, so a redelivered record is a visible no-op.
            $consumed = $this->interpreter->interpret($kafkaMessage);
            if (null === $consumed) {
                ++$skipped;
            } else {
                $this->bus->dispatch($consumed, idempotent: true);
                $output->writeln(sprintf('  <info>✓</info> %s', $this->describe($consumed)));
                ++$handled;
            }

            // Commit AFTER the handler (acknowledge = synchronous commit of this
            // record's offset) — at-least-once; a crash before this line redelivers
            // the record and the dedup middleware absorbs it. Skipped records are
            // acknowledged too: a record this consumer does not decode is done,
            // not pending.
            $consumer->acknowledge($record);
        }

        $context->close();

        $output->writeln('');
        $output->writeln(sprintf('<info>done</info> — handled %d, skipped %d', $handled, $skipped));

        return Command::SUCCESS;
    }

    /**
     * A one-line description of a consumed event for the run log: the wire name and
     * the order it touched (every consumed DTO is keyed on an order id).
     */
    private function describe(ConsumedMessage $message): string
    {
        $orderId = $message->dto instanceof OrderEvent ? $message->dto->orderId : '?';

        return sprintf('%s order=%s', $message->name, $orderId);
    }
}
