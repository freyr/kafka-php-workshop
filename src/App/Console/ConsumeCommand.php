<?php

declare(strict_types=1);

namespace Workshop\App\Console;

use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Workshop\App\Consumer\ConsoleWriter;
use Workshop\App\Consumer\ConsumedMessage;
use Workshop\App\Consumer\DecodedRecord;
use Workshop\App\Consumer\LatestSchemaResolver;
use Workshop\App\Consumer\MessageBus;
use Workshop\App\Consumer\MessageInterpreter;
use Workshop\App\Consumer\OrderEvent;
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
    public function __construct(
        private readonly ConsumerFactory $consumers,
        private readonly MessageInterpreter $interpreter,
        private readonly MessageBus $bus,
        private readonly ConsoleWriter $console,
        private readonly LatestSchemaResolver $readers,
    ) {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this
            ->addArgument('topic', InputArgument::REQUIRED, 'Topic to consume (e.g. enet.ecommerce.orders)')
            ->addOption('profile', null, InputOption::VALUE_REQUIRED, 'Consumer profile: ephemeral (throwaway, never commits, skips every record — inspect only) | default (background auto-commit, eager rebalancing) | modern (explicit commit, cooperative-sticky + static membership)', ConsumerProfile::Ephemeral->value)
            ->addOption('group', null, InputOption::VALUE_REQUIRED, 'Consumer group id (default/modern only; ephemeral always uses a fresh throwaway group). Omit to default to consume-<topic>')
            ->addOption('from', null, InputOption::VALUE_REQUIRED, 'Where to start: beginning | committed | end. Default committed (resume); ephemeral always reads from beginning', OffsetReset::Committed->value)
            ->addOption('idempotent', null, InputOption::VALUE_NONE, 'Wrap the handler in a DB transaction that dedups on event_id — effectively-once. Orthogonal to the profile; ignored by ephemeral (which never handles)')
            ->addOption('reader', null, InputOption::VALUE_REQUIRED, 'Schema to decode with: writer (each record in its own writer shape — old records keep their old fields) | latest (resolve every record against its subject\'s latest registered schema, filling fields added since from their defaults). default/modern only', 'writer')
            ->addOption('print', null, InputOption::VALUE_NONE, 'Print each record\'s DTO fields instead of projecting to the database — the Block 4 schema-evolution view. Bypasses the DB handler and its middleware')
            ->addOption('interval', null, InputOption::VALUE_REQUIRED, 'Milliseconds to pause between messages (throttle); default 0', '0')
            ->addOption('max', null, InputOption::VALUE_REQUIRED, 'Stop after this many messages (0 = no message cap)', '0')
            ->addOption('ttl', null, InputOption::VALUE_REQUIRED, 'Max lifetime in ms: stop after the consumer has lived this long, regardless of traffic (the time analogue of --max). Omit to run unbounded')
            ->addOption('drain', null, InputOption::VALUE_NONE, 'Stop at the first empty poll — read the backlog until drained, then exit (batch mode). Without it the consumer tails continuously, stopping only on --max, --ttl, or Ctrl-C');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        // Hand the run's output to the console sink so bus handlers that print
        // (FieldPrintHandler, the demo event's handler) have somewhere to write.
        $this->console->bind($output);

        try {
            $offsetReset = OffsetReset::fromOption(Input::string($input, 'from'));
            $lane = ConsumerProfile::fromOption(Input::string($input, 'profile'));
        } catch (\InvalidArgumentException $e) {
            $output->writeln('<error>' . $e->getMessage() . '</error>');

            return Command::INVALID;
        }

        $topic = Input::string($input, 'topic');
        $max = Input::int($input, 'max');
        $ttlMs = Input::intOrNull($input, 'ttl');
        $pauseMs = Input::int($input, 'interval');
        $groupOption = Input::stringOrNull($input, 'group');
        $idempotent = (bool) $input->getOption('idempotent');
        $drain = (bool) $input->getOption('drain');
        $print = (bool) $input->getOption('print');

        $readerMode = Input::string($input, 'reader');
        if (! in_array($readerMode, ['writer', 'latest'], true)) {
            $output->writeln('<error>--reader must be: writer | latest</error>');

            return Command::INVALID;
        }
        $latestReader = 'latest' === $readerMode;

        // Ephemeral is the throwaway inspector: it always reads the whole log from
        // the beginning and joins a unique, single-use group, so a committed offset
        // never enters the picture. The other lanes honor --from and a stable group.
        if ($lane->inspectsOnly()) {
            $offsetReset = OffsetReset::Beginning;
        }
        $group = $lane->inspectsOnly()
            ? sprintf('ephemeral-%s-%d-%d', $topic, getmypid(), time())
            : ($groupOption ?? sprintf('consume-%s', $topic));

        // Three independent stop conditions, all opt-in: --max (a count cap), --ttl (a
        // lifetime, below), and --drain (stop at the first empty poll). With none set
        // the consumer tails forever, ending only on a signal. The poll cadence is
        // fixed in MessageConsumer and is deliberately not configurable here.

        $output->writeln(sprintf(
            '<comment>topic=%s group=%s profile=%s from=%s reader=%s idempotent=%s ttl=%s mode=%s%s</comment>',
            $topic,
            $group,
            $lane->profileName(),
            $offsetReset->value,
            $readerMode,
            $idempotent ? 'yes' : 'no',
            null !== $ttlMs ? $ttlMs . 'ms' : '∞',
            $drain ? 'drain (stop on idle)' : 'tail',
            $print ? ' print=fields' : '',
        ));

        $narrate = $output->isVerbose()
            ? function (string $line) use ($output): void {
                $output->writeln('  <comment>' . $line . '</comment>');
            }
        : null;

        // The commit policy is decided once and handed to both the factory (so it
        // installs the offset-commit callback only when commits are async) and the
        // run-loop (so it commits the matching way) — the two can never disagree.
        $commitPolicy = $lane->commitPolicy($idempotent);

        // The factory assembles the consumer's callbacks (rebalance + error, plus the
        // offset-commit callback for the async policy) so the rebalance protocol stays
        // matched to the profile's assignment strategy.
        $consumer = $this->consumers->create($lane->profileName(), $group, $offsetReset, $narrate, commitPolicy: $commitPolicy);

        $tally = [
            'handled' => 0,
            'skipped' => 0,
        ];

        $messageHandler = $lane->inspectsOnly()
            ? $this->readOnlyHandler($output, $tally)
            : $this->dispatchingHandler($idempotent, $latestReader, $print, $output, $tally);

        $consumer->run(
            [$topic],
            $messageHandler,
            new RunLimits(maxMessages: $max, maxRuntimeMs: $ttlMs ?? 0, stopOnIdle: $drain),
            $commitPolicy,
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
     * The normal pipeline: decode each record, then denormalize it into a typed DTO
     * and dispatch it through the MessageBus, which routes the DTO to its one
     * registered handler (wrapped in transaction + dedup middleware when
     * $idempotent). $print is an orthogonal lens: it dumps the raw decoded record
     * (the wire fields, before the DTO) for every record — independent of which
     * handler runs, and visible even when the record then fails to hydrate its DTO.
     * $latestReader resolves each record against its subject's latest schema before
     * decoding (vs its own writer schema), so the two can be compared on one log.
     *
     * @param array{handled: int, skipped: int} $tally
     *
     * @return \Closure(\RdKafka\Message): void
     */
    private function dispatchingHandler(bool $idempotent, bool $latestReader, bool $print, OutputInterface $output, array &$tally): \Closure
    {
        return function (\RdKafka\Message $message) use ($idempotent, $latestReader, $print, $output, &$tally): void {
            // --reader=latest resolves this record's subject (via its message-name)
            // to the latest registered schema and decodes against it; otherwise the
            // interpreter decodes the record in its own writer shape.
            $reader = $latestReader && '' !== ($name = $this->header($message, 'message-name'))
                ? $this->readers->forMessageName($name)
                : null;

            $decoded = $this->interpreter->decode($message, $reader);
            if (null === $decoded) {
                if ($print) {
                    $output->writeln(sprintf(
                        '  <comment>•</comment> %s offset=%d — <error>skipped: not a record this consumer decodes</error>',
                        '' !== ($n = $this->header($message, 'message-name')) ? $n : '<none>',
                        $message->offset,
                    ));
                }
                ++$tally['skipped'];

                return;
            }

            // --print: show the raw decoded record (wire fields, incl. any the DTO
            // does not map) before it is shaped into the DTO.
            if ($print) {
                $this->printRaw($output, $decoded, $latestReader);
            }

            $consumed = $this->interpreter->denormalize($decoded);
            if (null === $consumed) {
                if ($print) {
                    $output->writeln(sprintf(
                        '      <error>skipped: could not read this record into the DTO under reader=%s</error>',
                        $latestReader ? 'latest' : 'writer',
                    ));
                }
                ++$tally['skipped'];

                return;
            }

            $this->bus->dispatch($consumed, $idempotent);
            ++$tally['handled'];
            $output->writeln(sprintf('  <info>✓</info> %s', $this->describe($consumed)));
        };
    }

    /**
     * Dump a decoded record's raw business fields (metadata stripped) as indented
     * JSON — the pre-DTO wire view that makes unmapped fields and schema drift
     * visible. Independent of the handler the DTO eventually routes to.
     */
    private function printRaw(OutputInterface $output, DecodedRecord $decoded, bool $latestReader): void
    {
        $output->writeln(sprintf(
            '  <info>•</info> %s offset=%d — raw decoded record (reader=%s):',
            $decoded->name,
            $decoded->offset,
            $latestReader ? 'latest' : 'writer',
        ));

        $json = json_encode($decoded->payload, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);
        foreach (explode("\n", false !== $json ? $json : '{}') as $line) {
            $output->writeln('      <comment>' . $line . '</comment>');
        }
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
        $orderId = $message->dto instanceof OrderEvent ? $message->dto->orderId : '?';

        return sprintf('%s order=%s', $message->name, $orderId);
    }
}
