<?php

declare(strict_types=1);

namespace Workshop\Console;

use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Workshop\Consume\ConsumedMessage;
use Workshop\Consume\ConsumerBus;
use Workshop\Consume\IdempotencyMiddleware;
use Workshop\Consume\MessageInterpreter;
use Workshop\Consume\OrderAuditedDto;
use Workshop\Consume\OrderCancelledDto;
use Workshop\Consume\OrderCreatedDto;
use Workshop\Consume\OrderUpdatedDto;
use Workshop\Consume\ProjectionHandler;
use Workshop\Consume\TransactionMiddleware;
use Workshop\Kafka\Callback\CallbackKit;
use Workshop\Kafka\Callback\ErrorCallback;
use Workshop\Kafka\Callback\RebalanceCallback;
use Workshop\Kafka\Client\ConsumerFactory;
use Workshop\Kafka\Runtime\ConsumerRunner;
use Workshop\Kafka\Runtime\ConsumeStrategy;
use Workshop\Kafka\Runtime\OffsetReset;
use Workshop\Kafka\Runtime\RunLimits;

#[AsCommand(
    name: 'kafka:consume',
    description: 'Consume a topic into the orders projection. Parametrize the group, start offset, throttle, and commit strategy (per-message / auto / idempotent).',
)]
final class ConsumeCommand extends Command
{
    /**
     * Poll interval (ms) when no --timeout is given; bounds how fast a tailing run reacts to Ctrl-C.
     */
    private const int TAIL_POLL_MS = 1000;

    public function __construct(
        private readonly ConsumerFactory $consumers,
        private readonly ConsumerRunner $runner,
        private readonly MessageInterpreter $interpreter,
        private readonly ProjectionHandler $handler,
        private readonly TransactionMiddleware $transaction,
        private readonly IdempotencyMiddleware $idempotency,
    ) {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this
            ->addArgument('topic', InputArgument::REQUIRED, 'Topic to consume (e.g. enet.ecommerce.orders)')
            ->addOption('group', 'g', InputOption::VALUE_REQUIRED, 'Consumer group id; omit for an ephemeral group that starts from earliest')
            ->addOption('from', null, InputOption::VALUE_REQUIRED, 'Where to start: beginning | committed | end', OffsetReset::Beginning->value)
            ->addOption('commit', null, InputOption::VALUE_REQUIRED, 'Mode: per-message | auto | idempotent | readonly (separate group, never commits, prints name/id only)', ConsumeStrategy::ReadOnly->value)
            ->addOption('interval', null, InputOption::VALUE_REQUIRED, 'Milliseconds to pause between messages (throttle); default 0', '0')
            ->addOption('auto-commit-interval', null, InputOption::VALUE_REQUIRED, 'Background commit interval in ms (only with --commit=auto); default 5000', '5000')
            ->addOption('max', 'm', InputOption::VALUE_REQUIRED, 'Stop after this many messages (0 = no message cap)', '0')
            ->addOption('timeout', 't', InputOption::VALUE_REQUIRED, 'Receive timeout in ms; an empty poll within it ends the run. Omit to tail continuously — stop only on --max or Ctrl-C')
            ->addOption('static-membership', null, InputOption::VALUE_NONE, 'Add a group.instance.id so a restart rejoins without a full rebalance; off by default — leave off for short CLI runs');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        try {
            $offsetReset = OffsetReset::fromOption(Input::string($input, 'from'));
            $strategy = ConsumeStrategy::fromOption(Input::string($input, 'commit'));
        } catch (\InvalidArgumentException $e) {
            $output->writeln('<error>' . $e->getMessage() . '</error>');

            return Command::INVALID;
        }

        $topic = Input::string($input, 'topic');
        $max = Input::int($input, 'max');
        $timeoutMs = Input::intOrNull($input, 'timeout');
        $pauseMs = Input::int($input, 'interval');
        $autoCommitMs = Input::int($input, 'auto-commit-interval');
        $groupOption = Input::stringOrNull($input, 'group');
        $staticMembership = (bool) $input->getOption('static-membership');

        // No --timeout → tail the topic continuously: keep polling forever and stop
        // only on --max or a signal. An explicit --timeout restores the read-until-idle
        // behavior, ending at the first empty poll within that window.
        $stopOnIdle = null !== $timeoutMs;
        $pollTimeoutMs = $timeoutMs ?? self::TAIL_POLL_MS;

        // Each lane maps to a named profile rather than a runtime tweak, so the config
        // it applies is visible in kafka:config:show:
        //   readonly      → ephemeral (throwaway, never commits)
        //   no -g         → ephemeral (throwaway group from earliest)
        //   named         → dynamic (dynamic membership: rebalance on every join/leave)
        //   named +static → at-least-once (group.instance.id: rejoin without rebalance)
        // The dynamic-vs-static pair is the rebalancing contrast for a named group.
        [$profile, $group] = match (true) {
            $strategy->isReadOnly() => ['consumer.ephemeral', sprintf('readonly-%s-%d-%d', $topic, getmypid(), time())],
            null === $groupOption => ['consumer.ephemeral', sprintf('ephemeral-%s-%d-%d', $topic, getmypid(), time())],
            $staticMembership => ['consumer.at-least-once', $groupOption],
            default => ['consumer.dynamic', $groupOption],
        };

        $output->writeln(sprintf(
            '<comment>topic=%s group=%s profile=%s from=%s commit=%s timeout=%s</comment>',
            $topic,
            $group,
            $profile,
            $offsetReset->value,
            $strategy->value,
            $stopOnIdle ? $timeoutMs . 'ms (stop on idle)' : '∞ (tail)',
        ));

        $narrate = $output->isVerbose()
            ? function (string $line) use ($output): void {
                $output->writeln('  <comment>' . $line . '</comment>');
            }
        : null;

        $callbacks = new CallbackKit(
            new RebalanceCallback($narrate, $offsetReset),
            new ErrorCallback($narrate),
        );

        $consumer = $this->consumers->create($profile, $group, $callbacks, $strategy->confOverrides($autoCommitMs));

        $tally = [
            'handled' => 0,
            'skipped' => 0,
        ];

        $messageHandler = $strategy->isReadOnly()
            ? $this->readOnlyHandler($output, $tally)
            : $this->dispatchingHandler($strategy, $output, $tally);

        $this->runner->run(
            $consumer,
            [$topic],
            $messageHandler,
            new RunLimits(maxMessages: $max, pollTimeoutMs: $pollTimeoutMs, stopOnIdle: $stopOnIdle),
            $strategy->commitPolicy(),
            $narrate,
            $pauseMs,
        );

        $output->writeln('');
        $output->writeln($strategy->isReadOnly()
            ? sprintf('<info>done</info> — inspected %d message(s)', $tally['handled'])
            : sprintf('<info>done</info> — handled %d, skipped %d', $tally['handled'], $tally['skipped']));

        return Command::SUCCESS;
    }

    /**
     * The normal pipeline: interpret each record into a typed DTO and dispatch it
     * through the bus (with the strategy's middleware) to the projection handler.
     *
     * @param array{handled: int, skipped: int} $tally
     *
     * @return \Closure(\RdKafka\Message): void
     */
    private function dispatchingHandler(ConsumeStrategy $strategy, OutputInterface $output, array &$tally): \Closure
    {
        $bus = new ConsumerBus(
            $this->handler,
            $strategy->isTransactional() ? [$this->transaction, $this->idempotency] : [],
        );

        return function (\RdKafka\Message $message) use ($bus, $output, &$tally): void {
            $consumed = $this->interpreter->interpret($message);
            if (null === $consumed) {
                ++$tally['skipped'];

                return;
            }

            $bus->dispatch($consumed);
            ++$tally['handled'];
            $output->writeln(sprintf('  <info>✓</info> %s', $this->describe($consumed)));
        };
    }

    /**
     * The readonly pipeline: print each record's name and id straight off the
     * headers — no decode, no DTO, no handler, no commit.
     *
     * @param array{handled: int, skipped: int} $tally
     *
     * @return \Closure(\RdKafka\Message): void
     */
    private function readOnlyHandler(OutputInterface $output, array &$tally): \Closure
    {
        return function (\RdKafka\Message $message) use ($output, &$tally): void {
            $output->writeln(sprintf(
                '  <info>•</info> %s id=%s partition=%d offset=%d',
                '' !== ($name = $this->header($message, 'message-name')) ? $name : '<none>',
                '' !== ($id = $this->header($message, 'event-id')) ? $id : '<none>',
                $message->partition,
                $message->offset,
            ));
            ++$tally['handled'];
        };
    }

    private function header(\RdKafka\Message $message, string $key): string
    {
        $value = $message->headers[$key] ?? null;

        return is_string($value) ? $value : '';
    }

    /**
     * A one-line description of a consumed event for the run log: the wire name and
     * the order it touched (every consumed DTO is keyed on an order id).
     */
    private function describe(ConsumedMessage $message): string
    {
        $orderId = match (true) {
            $message->dto instanceof OrderCreatedDto,
            $message->dto instanceof OrderUpdatedDto,
            $message->dto instanceof OrderCancelledDto,
            $message->dto instanceof OrderAuditedDto => $message->dto->orderId,
            default => '?',
        };

        return sprintf('%s order=%s', $message->name, $orderId);
    }
}
