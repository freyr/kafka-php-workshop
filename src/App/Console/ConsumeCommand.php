<?php

declare(strict_types=1);

namespace Workshop\App\Console;

use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Workshop\App\Consumer\ConsumedMessage;
use Workshop\App\Consumer\ConsumerBus;
use Workshop\App\Consumer\IdempotencyMiddleware;
use Workshop\App\Consumer\MessageInterpreter;
use Workshop\App\Consumer\OrderAuditedDto;
use Workshop\App\Consumer\OrderCancelledDto;
use Workshop\App\Consumer\OrderCreatedDto;
use Workshop\App\Consumer\OrderUpdatedDto;
use Workshop\App\Consumer\ProjectionHandler;
use Workshop\App\Consumer\TransactionMiddleware;
use Workshop\Kafka\Client\ConsumerFactory;
use Workshop\Kafka\Runtime\ConsumerProfile;
use Workshop\Kafka\Runtime\OffsetReset;
use Workshop\Kafka\Runtime\RunLimits;

#[AsCommand(
    name: 'kafka:consume',
    description: 'Consume a topic into the orders projection. Pick a consumer profile (ephemeral / default / modern), the start offset, and throttle; layer effectively-once on with --idempotent.',
)]
final class ConsumeCommand extends Command
{
    /**
     * Poll interval (ms) when no --timeout is given; bounds how fast a tailing run reacts to Ctrl-C.
     */
    private const int TAIL_POLL_MS = 1000;

    public function __construct(
        private readonly ConsumerFactory $consumers,
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
            ->addOption('profile', null, InputOption::VALUE_REQUIRED, 'Consumer profile: ephemeral (throwaway, never commits, skips every record — inspect only) | default (background auto-commit, eager rebalancing) | modern (explicit commit, cooperative-sticky + static membership)', ConsumerProfile::Ephemeral->value)
            ->addOption('group', 'g', InputOption::VALUE_REQUIRED, 'Consumer group id (default/modern only; ephemeral always uses a fresh throwaway group). Omit to default to consume-<topic>')
            ->addOption('from', null, InputOption::VALUE_REQUIRED, 'Where to start: beginning | committed | end. Default committed (resume); ephemeral always reads from beginning', OffsetReset::Committed->value)
            ->addOption('idempotent', null, InputOption::VALUE_NONE, 'Wrap the handler in a DB transaction that dedups on event_id — effectively-once. Orthogonal to the profile; ignored by ephemeral (which never handles)')
            ->addOption('interval', null, InputOption::VALUE_REQUIRED, 'Milliseconds to pause between messages (throttle); default 0', '0')
            ->addOption('auto-commit-interval', null, InputOption::VALUE_REQUIRED, 'Background commit interval in ms (only with --profile=default); default 5000', '5000')
            ->addOption('max', 'm', InputOption::VALUE_REQUIRED, 'Stop after this many messages (0 = no message cap)', '0')
            ->addOption('timeout', 't', InputOption::VALUE_REQUIRED, 'Receive timeout in ms; an empty poll within it ends the run. Omit to tail continuously — stop only on --max or Ctrl-C');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        try {
            $offsetReset = OffsetReset::fromOption(Input::string($input, 'from'));
            $lane = ConsumerProfile::fromOption(Input::string($input, 'profile'));
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
        $idempotent = (bool) $input->getOption('idempotent');

        // Ephemeral is the throwaway inspector: it always reads the whole log from
        // the beginning and joins a unique, single-use group, so a committed offset
        // never enters the picture. The other lanes honor --from and a stable group.
        if ($lane->inspectsOnly()) {
            $offsetReset = OffsetReset::Beginning;
        }
        $group = $lane->inspectsOnly()
            ? sprintf('ephemeral-%s-%d-%d', $topic, getmypid(), time())
            : ($groupOption ?? sprintf('consume-%s', $topic));

        // No --timeout → tail the topic continuously: keep polling forever and stop
        // only on --max or a signal. An explicit --timeout restores the read-until-idle
        // behavior, ending at the first empty poll within that window.
        $stopOnIdle = null !== $timeoutMs;
        $pollTimeoutMs = $timeoutMs ?? self::TAIL_POLL_MS;

        // The lane names the KafkaProfile (the librdkafka config). The only runtime
        // override is the default lane's auto-commit interval — every other config
        // difference (commit mode, rebalancing, static membership) is in the profile.
        $overrides = ConsumerProfile::DefaultLane === $lane
            ? [
                'auto.commit.interval.ms' => (string) $autoCommitMs,
            ]
            : [];

        $output->writeln(sprintf(
            '<comment>topic=%s group=%s profile=%s from=%s idempotent=%s timeout=%s</comment>',
            $topic,
            $group,
            $lane->profileName(),
            $offsetReset->value,
            $idempotent ? 'yes' : 'no',
            $stopOnIdle ? $timeoutMs . 'ms (stop on idle)' : '∞ (tail)',
        ));

        $narrate = $output->isVerbose()
            ? function (string $line) use ($output): void {
                $output->writeln('  <comment>' . $line . '</comment>');
            }
        : null;

        // The factory assembles the consumer's callbacks (rebalance + error) so the
        // rebalance protocol stays matched to the profile's assignment strategy.
        $consumer = $this->consumers->create($lane->profileName(), $group, $offsetReset, $narrate, $overrides);

        $tally = [
            'handled' => 0,
            'skipped' => 0,
        ];

        $messageHandler = $lane->inspectsOnly()
            ? $this->readOnlyHandler($output, $tally)
            : $this->dispatchingHandler($idempotent, $output, $tally);

        $consumer->run(
            [$topic],
            $messageHandler,
            new RunLimits(maxMessages: $max, pollTimeoutMs: $pollTimeoutMs, stopOnIdle: $stopOnIdle),
            $lane->commitPolicy($idempotent),
            $narrate,
            $pauseMs,
        );

        $output->writeln('');
        $output->writeln($lane->inspectsOnly()
            ? sprintf('<info>done</info> — inspected %d message(s)', $tally['handled'])
            : sprintf('<info>done</info> — handled %d, skipped %d', $tally['handled'], $tally['skipped']));

        return Command::SUCCESS;
    }

    /**
     * The normal pipeline: interpret each record into a typed DTO and dispatch it
     * through the bus to the projection handler. With $idempotent the dispatch is
     * wrapped in the transaction + dedup middleware (effectively-once) — an
     * orthogonal handler/DB concern, independent of the profile's Kafka config.
     *
     * @param array{handled: int, skipped: int} $tally
     *
     * @return \Closure(\RdKafka\Message): void
     */
    private function dispatchingHandler(bool $idempotent, OutputInterface $output, array &$tally): \Closure
    {
        $bus = new ConsumerBus(
            $this->handler,
            $idempotent ? [$this->transaction, $this->idempotency] : [],
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
