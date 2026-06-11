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
use Workshop\App\Consumer\ErrorDemoDto;
use Workshop\App\Consumer\ErrorRouter;
use Workshop\App\Consumer\LatestSchemaResolver;
use Workshop\App\Consumer\MessageBus;
use Workshop\App\Consumer\MessageInterpreter;
use Workshop\App\Consumer\OrderEvent;
use Workshop\Kafka\Client\ConsumerFactory;
use Workshop\Kafka\Client\ProducerFactory;
use Workshop\Kafka\Runtime\CircuitBreaker;
use Workshop\Kafka\Runtime\ConsumerInterrupted;
use Workshop\Kafka\Runtime\ConsumerProfile;
use Workshop\Kafka\Runtime\ErrorPolicy;
use Workshop\Kafka\Runtime\OffsetReset;
use Workshop\Kafka\Runtime\PoisonMessageException;
use Workshop\Kafka\Runtime\RunLimits;
use Workshop\Kafka\Runtime\TransientException;

#[AsCommand(
    name: 'kafka:consume',
    description: 'Consume a topic into the orders projection. Pick a consumer profile (ephemeral / default / modern), the start offset, and throttle; layer effectively-once on with --idempotent.',
)]
final class ConsumeCommand extends Command
{
    public function __construct(
        private readonly ConsumerFactory $consumers,
        private readonly ProducerFactory $producers,
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
            ->addOption('drain', null, InputOption::VALUE_NONE, 'Stop at the first empty poll — read the backlog until drained, then exit (batch mode). Without it the consumer tails continuously, stopping only on --max, --ttl, or Ctrl-C')
            ->addOption('errors', null, InputOption::VALUE_REQUIRED, 'Error-handling lane (Block 7): off (default — tolerant null-skips, no routing) | main (3 in-process retries @ 1s/2s/4s, then off-load to <topic>.retry; poison/permanent → <topic>.dlq; breaker fails fast) | slow (unbounded retries, 5s doubling capped 60s; breaker pauses — for consuming the .retry topic)', ErrorPolicy::Off->value);
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        // Hand the run's output to the console sink so bus handlers that print
        // (FieldPrintHandler, the demo event's handler) have somewhere to write.
        $this->console->bind($output);

        try {
            $offsetReset = OffsetReset::fromOption(Input::string($input, 'from'));
            $lane = ConsumerProfile::fromOption(Input::string($input, 'profile'));
            $errors = ErrorPolicy::fromOption(Input::string($input, 'errors'));
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

        // The error lanes route through the bus and commit after routing, which
        // neither the inspect-only profile nor the view-only --print ever do.
        if ($errors->enabled() && $lane->inspectsOnly()) {
            $output->writeln('<error>--errors needs a handling profile (default | modern) — the ephemeral inspector never decodes or dispatches.</error>');

            return Command::INVALID;
        }
        if ($errors->enabled() && $print) {
            $output->writeln('<error>--errors and --print are mutually exclusive — --print bypasses the bus the error lanes wrap.</error>');

            return Command::INVALID;
        }

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
            '<comment>topic=%s group=%s profile=%s from=%s reader=%s idempotent=%s ttl=%s mode=%s%s%s</comment>',
            $topic,
            $group,
            $lane->profileName(),
            $offsetReset->value,
            $readerMode,
            $idempotent ? 'yes' : 'no',
            null !== $ttlMs ? $ttlMs . 'ms' : '∞',
            $drain ? 'drain (stop on idle)' : 'tail',
            $print ? ' print=fields' : '',
            $errors->enabled() ? ' errors=' . $errors->value : '',
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
            'retried' => 0,
            'offloaded' => 0,
            'dead-lettered' => 0,
        ];

        // The abort flag the error lanes' retry loops poll: the run loop's own
        // signal handler flips it (via $onSignal), so a Ctrl+C mid-retry breaks
        // the loop instead of waiting out a 60s backoff.
        $aborted = false;
        $onSignal = $errors->enabled()
            ? function () use (&$aborted): void {
                $aborted = true;
            }
        : null;

        $messageHandler = match (true) {
            $lane->inspectsOnly() => $this->readOnlyHandler($output, $tally),
            $errors->enabled() => $this->errorHandlingHandler($errors, $topic, $idempotent, $latestReader, $output, $tally, $aborted),
            default => $this->dispatchingHandler($idempotent, $latestReader, $print, $output, $tally),
        };

        try {
            $consumer->run(
                [$topic],
                $messageHandler,
                new RunLimits(maxMessages: $max, maxRuntimeMs: $ttlMs ?? 0, stopOnIdle: $drain),
                $commitPolicy,
                $narrate,
                $pauseMs,
                $onSignal,
            );
        } catch (ConsumerInterrupted) {
            // Returning from the handler would have committed an unhandled
            // message; the exception path leaves the offset uncommitted instead.
            $output->writeln('');
            $output->writeln('<comment>interrupted mid-retry — offset uncommitted, the message redelivers on the next run</comment>');
            $this->report($lane, $errors, $tally, $output);

            return Command::SUCCESS;
        }

        $output->writeln('');
        $this->report($lane, $errors, $tally, $output);

        return Command::SUCCESS;
    }

    /**
     * @param array{handled: int, skipped: int, retried: int, offloaded: int, dead-lettered: int} $tally
     */
    private function report(ConsumerProfile $lane, ErrorPolicy $errors, array $tally, OutputInterface $output): void
    {
        if ($lane->inspectsOnly()) {
            $output->writeln(sprintf('<info>done</info> — inspected %d message(s)', $tally['handled']));

            return;
        }
        if ($errors->enabled()) {
            $output->writeln(sprintf(
                '<info>done</info> — handled %d, skipped %d, retried %d, off-loaded %d, dead-lettered %d',
                $tally['handled'],
                $tally['skipped'],
                $tally['retried'],
                $tally['offloaded'],
                $tally['dead-lettered'],
            ));

            return;
        }

        $output->writeln(sprintf('<info>done</info> — handled %d, skipped %d', $tally['handled'], $tally['skipped']));
    }

    /**
     * The normal pipeline: decode each record, then denormalize it into a typed DTO
     * and dispatch it through the MessageBus, which routes the DTO to its one
     * registered handler (wrapped in transaction + dedup middleware when
     * $idempotent). $print replaces dispatch with a view-only lens: it dumps the raw
     * decoded record (the wire fields, before the DTO) and skips the bus entirely —
     * no DB handler, no middleware — while still surfacing decode/hydration skips.
     * $latestReader resolves each record against its subject's latest schema before
     * decoding (vs its own writer schema), so the two can be compared on one log.
     *
     * @param array{handled: int, skipped: int, retried: int, offloaded: int, dead-lettered: int} $tally
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

            // --print is a view-only lens: the raw dump above already showed the
            // record, so the DTO is never dispatched — no DB handler, no middleware
            // (the option's documented contract).
            if (! $print) {
                $this->bus->dispatch($consumed, $idempotent);
                $output->writeln(sprintf('  <info>✓</info> %s', $this->describe($consumed)));
            }
            ++$tally['handled'];
        };
    }

    /**
     * The Block 7 pipeline: decode with the poison gate armed, then dispatch under
     * the policy's retry budget and circuit breaker, routing every failure off the
     * partition — poison and permanent failures to <topic>.dlq, exhausted (or
     * breaker-fail-fast) transients to <topic>.retry. The closure always RETURNS
     * after routing (never rethrows a handling failure), so the run loop commits
     * the offset and the partition advances — the cardinal rule. The only
     * exception out of here is ConsumerInterrupted: a Ctrl+C mid-retry must NOT
     * commit, so it travels as a throw.
     *
     * Narration writes straight to $output (not the -v narrator): watching the
     * routing decisions IS the demo.
     *
     * @param array{handled: int, skipped: int, retried: int, offloaded: int, dead-lettered: int} $tally
     *
     * @return \Closure(\RdKafka\Message): void
     */
    private function errorHandlingHandler(
        ErrorPolicy $policy,
        string $topic,
        bool $idempotent,
        bool $latestReader,
        OutputInterface $output,
        array &$tally,
        bool &$aborted,
    ): \Closure {
        $router = new ErrorRouter($this->producers->createRaw(
            'producer.dlq',
            $output->isVerbose() ? static fn (string $line) => $output->writeln('  <comment>' . $line . '</comment>') : null,
        ));
        $breaker = new CircuitBreaker($policy->breakerThreshold(), $policy->breakerCooldownMs());

        return function (\RdKafka\Message $message) use ($policy, $router, $breaker, $topic, $idempotent, $latestReader, $output, &$tally, &$aborted): void {
            try {
                $reader = $latestReader && '' !== ($name = $this->header($message, 'message-name'))
                    ? $this->readers->forMessageName($name)
                    : null;
                $decoded = $this->interpreter->decode($message, $reader, poisonGate: true);
            } catch (PoisonMessageException $e) {
                $destination = $router->deadLetter($message, $e, ErrorRouter::REASON_POISON, $topic);
                $output->writeln(sprintf('  <fg=red>☠ poison</> partition=%d offset=%d → <comment>%s</comment> (%s)', $message->partition, $message->offset, $destination, $e->getMessage()));
                ++$tally['dead-lettered'];

                return; // routed — the run loop commits, the partition advances
            }
            if (null === $decoded) {
                ++$tally['skipped'];

                return; // not a type this consumer handles — tolerate, as ever
            }

            $consumed = $this->interpreter->denormalize($decoded);
            if (null === $consumed) {
                ++$tally['skipped'];

                return; // DTO-hydration drift stays a skip (the Block 4 contract)
            }

            $attempt = 0;
            $max = $policy->maxAttempts();

            while (true) {
                if ($aborted) {
                    throw new ConsumerInterrupted('signal received mid-retry');
                }

                $nowMs = $this->nowMs();
                if (! $breaker->allowsAttempt($nowMs)) {
                    if ($policy->pausesWhenOpen()) {
                        // Slow lane: the topic's job is to wait — pause out the
                        // cooldown, then the next iteration probes half-open.
                        $remaining = $breaker->remainingCooldownMs($nowMs);
                        $output->writeln(sprintf('  <fg=yellow>◌ breaker OPEN</> — pausing %.1fs before the half-open probe', $remaining / 1000));
                        $this->sleepAbortAware($remaining, $aborted);

                        continue;
                    }

                    // Main lane: fail fast — no attempt, no in-process retries; the
                    // message moves off the hot path immediately and the struggling
                    // dependency stops being hammered.
                    $destination = $router->offloadToRetry($message, new TransientException('circuit breaker open — failing fast'), $topic);
                    $output->writeln(sprintf('  <fg=yellow>↯ breaker OPEN</> — fail fast: %s → <comment>%s</comment> (x-retry-count=%d)', $this->describe($consumed), $destination, $router->retryCount($message) + 1));
                    ++$tally['offloaded'];

                    return;
                }

                if ($breaker->isHalfOpen($nowMs)) {
                    $output->writeln('  <fg=yellow>◌ breaker HALF-OPEN</> — probing with the next message');
                }

                ++$attempt;

                try {
                    $this->bus->dispatch($consumed, $idempotent);
                    if ($breaker->onSuccess()) {
                        $output->writeln('  <info>● breaker CLOSED</info> — probe succeeded');
                    }
                    $output->writeln(sprintf('  <info>✓</info> %s%s', $this->describe($consumed), $attempt > 1 ? sprintf(' (succeeded on attempt %d)', $attempt) : ''));
                    ++$tally['handled'];

                    return;
                } catch (TransientException $e) {
                    if ($breaker->onTransientFailure($this->nowMs())) {
                        $output->writeln(sprintf('  <fg=yellow>↯ breaker OPEN</> — %d consecutive transient failures (cooldown %.1fs)', max($breaker->consecutiveFailures(), $policy->breakerThreshold()), $policy->breakerCooldownMs() / 1000));
                    }

                    if (null !== $max && $attempt >= $max) {
                        $destination = $router->offloadToRetry($message, $e, $topic);
                        $output->writeln(sprintf('  <fg=yellow>↻ transient persisted</> after %d attempts: %s → <comment>%s</comment> (x-retry-count=%d)', $attempt, $this->describe($consumed), $destination, $router->retryCount($message) + 1));
                        ++$tally['offloaded'];

                        return;
                    }

                    if ($breaker->isOpen()) {
                        continue; // the open-breaker branch decides what happens next
                    }

                    $delayMs = $policy->retryDelayMs($attempt);
                    $output->writeln(sprintf('  <fg=yellow>↻ transient</> (attempt %d%s) %s — retrying in %.1fs', $attempt, null !== $max ? '/' . $max : '', $e->getMessage(), $delayMs / 1000));
                    ++$tally['retried'];
                    $this->sleepAbortAware($delayMs, $aborted);
                } catch (\Throwable $e) {
                    // Anything not explicitly transient is permanent: retrying an
                    // unknown error gambles with partition liveness, and the DLQ
                    // is recoverable by replay.
                    $destination = $router->deadLetter($message, $e, ErrorRouter::REASON_PERMANENT, $topic);
                    $output->writeln(sprintf('  <fg=red>☠ permanent</> %s: %s → <comment>%s</comment>', $this->describe($consumed), $e->getMessage(), $destination));
                    ++$tally['dead-lettered'];

                    return;
                }
            }
        };
    }

    /**
     * Sleep in short slices so a SIGINT/SIGTERM (which flips $aborted via the run
     * loop's signal handler) cuts a long backoff short instead of waiting it out.
     */
    private function sleepAbortAware(int $ms, bool &$aborted): void
    {
        $sliceMs = 200;
        while ($ms > 0 && ! $aborted) {
            usleep(min($ms, $sliceMs) * 1000);
            $ms -= $sliceMs;
        }
    }

    private function nowMs(): int
    {
        return intdiv(hrtime(true), 1_000_000);
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
     * @param array{handled: int, skipped: int, retried: int, offloaded: int, dead-lettered: int} $tally
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
     * A one-line description of a consumed event for the run log: the wire name
     * plus the identity its DTO carries (order id for the order events, seq+id
     * for the Block 7 error.demo event).
     */
    private function describe(ConsumedMessage $message): string
    {
        return match (true) {
            $message->dto instanceof OrderEvent => sprintf('%s order=%s', $message->name, $message->dto->orderId),
            $message->dto instanceof ErrorDemoDto => sprintf('%s seq=%d id=%s', $message->name, $message->dto->seq, $message->dto->id),
            default => sprintf('%s order=?', $message->name),
        };
    }
}
