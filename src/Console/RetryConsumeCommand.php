<?php

declare(strict_types=1);

namespace Workshop\Console;

use Doctrine\DBAL\Connection;
use Enqueue\RdKafka\RdKafkaMessage;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Workshop\Kernel\AvroEventSerializer;
use Workshop\Kernel\Database;
use Workshop\Kernel\IdempotencyStore;
use Workshop\Kernel\KafkaContextFactory;
use Workshop\Kernel\PoisonMessageException;
use Workshop\Kernel\RetryRouter;
use Workshop\Kernel\SideEffectStore;
use Workshop\Kernel\TransientException;

#[AsCommand(
    name: 'retry:consume',
    description: 'Block 7 demo: a bounded consumer that classifies failures and routes them — poison → DLT, transient → in-process retry then the retry-topic chain — so the partition is never blocked. --naive drops the routing to show a stuck partition.',
)]
final class RetryConsumeCommand extends Command
{
    public function __construct(
        private readonly KafkaContextFactory $kafka,
        private readonly AvroEventSerializer $avro,
        private readonly RetryRouter $router,
        private readonly Database $db,
        private readonly IdempotencyStore $idempotency,
        private readonly SideEffectStore $sideEffects,
    ) {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this
            ->addArgument('topic', InputArgument::OPTIONAL, 'Topic to consume (point it at a .retry.* tier to drain that tier)', 'enet.ecommerce.orders')
            ->addOption('group', 'g', InputOption::VALUE_REQUIRED, 'Consumer group', 'retry-demo')
            ->addOption('poison', null, InputOption::VALUE_REQUIRED, 'Comma-separated order ids to treat as POISON (non-retryable → DLT immediately)')
            ->addOption('flaky', null, InputOption::VALUE_REQUIRED, 'Comma-separated order ids that fail transiently until they have been through one retry hop, then succeed')
            ->addOption('naive', null, InputOption::VALUE_NONE, 'No routing: retry in-process then exit WITHOUT acking — the stuck-partition anti-pattern')
            ->addOption('in-process-retries', null, InputOption::VALUE_REQUIRED, 'In-process retry attempts for a transient error before routing to a retry topic', 3)
            ->addOption('max', 'm', InputOption::VALUE_REQUIRED, 'Stop after receiving this many messages (0 = until the receive timeout)', 0)
            ->addOption('timeout', 't', InputOption::VALUE_REQUIRED, 'Receive timeout in ms (also the signal-responsiveness window)', 2000)
            ->addOption('memory-limit', null, InputOption::VALUE_REQUIRED, 'Exit cleanly once RSS passes this many MB (a supervisor restarts the process)', 256);
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $topic = (string) $input->getArgument('topic');
        $group = (string) $input->getOption('group');
        $poison = $this->idSet((string) $input->getOption('poison'));
        $flaky = $this->idSet((string) $input->getOption('flaky'));
        $naive = (bool) $input->getOption('naive');
        $inProcessRetries = max(1, (int) $input->getOption('in-process-retries'));
        $max = (int) $input->getOption('max');
        $timeoutMs = (int) $input->getOption('timeout');
        $memoryLimitBytes = (int) $input->getOption('memory-limit') * 1024 * 1024;

        $this->db->ensureSchema();

        // Graceful shutdown: flip $running on SIGTERM/SIGINT and let the loop
        // finish the current message and commit before exiting. async signals
        // make the handler fire even while blocked in receive().
        $running = true;
        pcntl_async_signals(true);
        $stop = function (int $signal) use (&$running, $output): void {
            $output->writeln(sprintf('<comment>received %s — finishing current message, then shutting down…</comment>', SIGTERM === $signal ? 'SIGTERM' : 'SIGINT'));
            $running = false;
        };
        pcntl_signal(SIGTERM, $stop);
        pcntl_signal(SIGINT, $stop);

        $output->writeln(sprintf('<comment>topic=%s group=%s mode=%s</comment>', $topic, $group, $naive ? 'NAIVE (no routing)' : 'bounded'));

        $context = $this->kafka->forConsumer($group);
        $consumer = $context->createConsumer($context->createTopic($topic));
        $consumer->setCommitAsync(false);

        $applied = 0;
        $skipped = 0;
        $dead = 0;
        $routed = 0;
        $received = 0;

        while ($running && (0 === $max || $received < $max)) {
            $message = $consumer->receive($timeoutMs);
            if (null === $message) {
                // Nothing within the timeout window. Once we have seen at least one
                // message, treat a quiet topic as drained and exit (a production
                // consumer would instead loop forever under a supervisor). Before
                // the first message, keep waiting through the initial rebalance.
                if ($received > 0 || ! $running) {
                    break;
                }
                continue;
            }
            if (! $message instanceof RdKafkaMessage) {
                continue;
            }
            ++$received;

            $this->enforceRetryDelay($message, $output);

            $currentRetry = (int) ($message->getHeader('x-retry-count') ?? 0);

            // Deserialization is its own category: the bytes arrived but cannot be
            // decoded (unknown schema id, wrong format). Never retryable here —
            // it is a schema-contract problem a human must fix.
            try {
                $event = $this->avro->decode($message->getBody());
            } catch (\Throwable $e) {
                if ($naive) {
                    $this->reportStuck($message, $output);
                    $context->close();

                    return Command::SUCCESS;
                }
                $dest = $this->router->deadLetter($message, $e, 'deserialization_error', $currentRetry);
                $output->writeln(sprintf('  <error>✗ DESERIALIZE failed → %s</error> + ALERT (schema contract broken)', $dest));
                $consumer->acknowledge($message);
                ++$dead;
                continue;
            }

            $metadata = $event['metadata'] ?? [];
            $eventId = (string) ($metadata['event_id'] ?? '');
            $eventType = (string) ($metadata['event_type'] ?? 'unknown');
            $orderId = (string) ($metadata['aggregate_id'] ?? '?');

            try {
                $didApply = $this->processWithInProcessRetry(
                    fn (): bool => $this->process($orderId, $eventId, $eventType, $poison, $flaky, $currentRetry),
                    $inProcessRetries,
                    $orderId,
                    $output,
                );

                if ($didApply) {
                    ++$applied;
                    $output->writeln(sprintf('  <info>✓ APPLIED</info> order=%s event=%s', $orderId, $this->short($eventId)));
                } else {
                    ++$skipped;
                    $output->writeln(sprintf('  <comment>↩ skip duplicate</comment> order=%s event=%s', $orderId, $this->short($eventId)));
                }
            } catch (PoisonMessageException $e) {
                if ($naive) {
                    $this->reportStuck($message, $output);
                    $context->close();

                    return Command::SUCCESS;
                }
                $dest = $this->router->deadLetter($message, $e, 'poison_message', $currentRetry);
                $output->writeln(sprintf('  <error>☠ POISON</error> order=%s → %s (0 retries)', $orderId, $dest));
                ++$dead;
            } catch (TransientException $e) {
                if ($naive) {
                    $this->reportStuck($message, $output);
                    $context->close();

                    return Command::SUCCESS;
                }
                $dest = $this->router->retry($message, $e, $currentRetry);
                $output->writeln(sprintf('  <comment>↻ TRANSIENT</comment> order=%s (in-process exhausted) → %s', $orderId, $dest));
                ++$routed;
            }

            // Ack only after the message either succeeded or was durably routed
            // (the router flushed to the broker first). The partition advances —
            // this is the whole anti-partition-blocking rule.
            $consumer->acknowledge($message);

            if (memory_get_usage(true) > $memoryLimitBytes) {
                $output->writeln(sprintf('<comment>memory limit reached (%d MB) — exiting for a supervisor restart</comment>', (int) round(memory_get_usage(true) / 1024 / 1024)));
                break;
            }
        }

        $context->close();
        $this->summary($output, $applied, $skipped, $routed, $dead);

        return Command::SUCCESS;
    }

    /**
     * The simulated unit of work. Poison ids always fail non-retryably; flaky ids
     * fail transiently until the message has taken at least one retry hop
     * (x-retry-count >= 1) — modelling a transient that outlives in-process retry
     * but clears by the time the retry tier re-delivers it. Otherwise the work
     * succeeds and applies the Block 5 side-effect idempotently.
     *
     * @param array<string, true> $poison
     * @param array<string, true> $flaky
     *
     * @return bool true if the side-effect was applied, false if it was a duplicate
     */
    private function process(string $orderId, string $eventId, string $eventType, array $poison, array $flaky, int $currentRetry): bool
    {
        if (isset($poison[$orderId])) {
            throw new PoisonMessageException(sprintf('order %s references a product that does not exist', $orderId));
        }

        if (isset($flaky[$orderId]) && $currentRetry < 1) {
            throw new TransientException(sprintf('order %s: inventory service returned HTTP 503', $orderId));
        }

        return $this->db->transactional(function (Connection $tx) use ($orderId, $eventId, $eventType): bool {
            if ('' !== $eventId && ! $this->idempotency->recordIfNew($tx, $eventId, $eventType)) {
                return false; // already processed — replay/duplicate, no-op
            }

            $this->sideEffects->apply($tx, $orderId, $eventId);

            return true;
        });
    }

    /**
     * Run $work, retrying only TransientException in-process with exponential
     * backoff (1s, 2s, 4s…). Poison propagates immediately — zero retries. After
     * the budget is spent the TransientException escapes so the caller can route
     * it to a retry topic.
     *
     * @param callable(): bool $work
     */
    private function processWithInProcessRetry(callable $work, int $maxRetries, string $orderId, OutputInterface $output): bool
    {
        for ($attempt = 1; $attempt <= $maxRetries; ++$attempt) {
            try {
                return $work();
            } catch (TransientException $e) {
                if ($attempt === $maxRetries) {
                    throw $e;
                }
                $delay = 2 ** ($attempt - 1);
                $output->writeln(sprintf('  <comment>… transient on order=%s (attempt %d/%d): retrying in %ds</comment>', $orderId, $attempt, $maxRetries, $delay));
                sleep($delay);
            }
        }

        return false; // unreachable; the loop either returns or throws
    }

    /**
     * Honour x-next-retry-after when draining a retry tier: low-throughput retry
     * topics can simply sleep out the remaining delay (research §2.2 Option A).
     */
    private function enforceRetryDelay(RdKafkaMessage $message, OutputInterface $output): void
    {
        $retryAfter = (int) ($message->getHeader('x-next-retry-after') ?? 0);
        $wait = $retryAfter - time();
        if ($wait > 0) {
            $output->writeln(sprintf('  <comment>⏲ retry delay: waiting %ds</comment>', $wait));
            sleep($wait);
        }
    }

    private function reportStuck(RdKafkaMessage $message, OutputInterface $output): void
    {
        $kafka = $message->getKafkaMessage();
        $output->writeln(sprintf(
            '<error>💥 message failed and NAIVE mode has no DLT — NOT acking.</error>',
        ));
        $output->writeln(sprintf(
            '<comment>partition %s is stuck on offset %s; later messages are never processed. Rerun and it redelivers the same message.</comment>',
            $kafka?->partition ?? '?',
            $kafka?->offset ?? '?',
        ));
    }

    /**
     * @return array<string, true>
     */
    private function idSet(string $csv): array
    {
        $set = [];
        foreach (array_filter(array_map('trim', explode(',', $csv))) as $id) {
            $set[$id] = true;
        }

        return $set;
    }

    private function summary(OutputInterface $output, int $applied, int $skipped, int $routed, int $dead): void
    {
        $conn = $this->db->connection();
        $output->writeln('');
        $output->writeln(sprintf(
            'applied=%d skipped=%d routed-to-retry=%d dead-lettered=%d · side_effects rows=%d',
            $applied,
            $skipped,
            $routed,
            $dead,
            $this->sideEffects->count($conn),
        ));
    }

    private function short(string $eventId): string
    {
        return '' === $eventId ? '<none>' : substr($eventId, 0, 8);
    }
}
