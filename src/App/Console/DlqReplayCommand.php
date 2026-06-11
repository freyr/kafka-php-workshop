<?php

declare(strict_types=1);

namespace Workshop\App\Console;

use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Workshop\Kafka\Client\ConsumerFactory;
use Workshop\Kafka\Client\ProducerFactory;
use Workshop\Kafka\Runtime\CommitPolicy;
use Workshop\Kafka\Runtime\OffsetReset;
use Workshop\Kafka\Runtime\RunLimits;

#[AsCommand(
    name: 'kafka:dlq:replay',
    description: 'Re-publish every DLQ message to its x-original-topic — the recovery step after the cause is fixed. Safe to re-run: replays keep their event-id, so the idempotent consumer skips duplicates.',
)]
final class DlqReplayCommand extends Command
{
    /**
     * Per-hop diagnostics stripped on replay: the message starts a fresh life on
     * its original topic. The event-id and message-name headers survive — they
     * are what make replay routable and dedup-safe.
     */
    private const array DIAGNOSTICS = [
        'x-dead-letter-reason',
        'x-error-class',
        'x-error-message',
        'x-original-topic',
        'x-original-partition',
        'x-original-offset',
        'x-retry-count',
        'x-failed-at',
    ];

    public function __construct(
        private readonly ConsumerFactory $consumers,
        private readonly ProducerFactory $producers,
    ) {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this
            ->addArgument('topic', InputArgument::OPTIONAL, 'DLQ topic to replay from', 'enet.ecommerce.outbox.ErrorDemo.dlq')
            ->addOption('dry-run', null, InputOption::VALUE_NONE, 'Show what would be replayed where, produce nothing');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $topic = Input::string($input, 'topic');
        $dryRun = (bool) $input->getOption('dry-run');

        $narrate = $output->isVerbose()
            ? static fn (string $line) => $output->writeln('  <comment>' . $line . '</comment>')
            : null;

        $producer = $dryRun ? null : $this->producers->createRaw('producer.dlq', $narrate);

        // The DLQ is read with a throwaway group and never committed: replay
        // leaves the DLQ untouched (it is the audit trail), and re-running replays
        // everything again — harmless, because the consumer dedups on event-id.
        $consumer = $this->consumers->create(
            'consumer.ephemeral',
            sprintf('dlq-replay-%s-%d-%d', $topic, getmypid(), time()),
            OffsetReset::Beginning,
            $narrate,
        );

        $output->writeln(sprintf('<comment>replaying %s%s</comment>', $topic, $dryRun ? ' (dry run — nothing is produced)' : ''));

        $replayed = 0;
        $skipped = 0;
        $consumer->run(
            [$topic],
            function (\RdKafka\Message $message) use ($producer, $dryRun, $output, &$replayed, &$skipped): void {
                $destination = $this->header($message, 'x-original-topic');
                if ('' === $destination) {
                    ++$skipped;
                    $output->writeln(sprintf('  <error>✗</error> offset=%d has no x-original-topic header — cannot route, leaving it in the DLQ', $message->offset));

                    return;
                }

                $output->writeln(sprintf(
                    '  %s %s id=%s → <comment>%s</comment>%s',
                    $dryRun ? '<comment>would replay</comment>' : '<info>↩ replayed</info>',
                    $this->header($message, 'message-name', '<none>'),
                    $this->header($message, 'event-id', '<none>'),
                    $destination,
                    $dryRun ? '' : sprintf(' (key=%s preserved)', $this->keyLabel($message->key)),
                ));
                ++$replayed;

                if ($dryRun || null === $producer) {
                    return;
                }

                $producer->produce(
                    $destination,
                    $message->key,
                    (string) $message->payload,
                    [
                        ...$this->replayHeaders($message),
                        'x-replayed-from-dlq' => gmdate('Y-m-d\TH:i:s\Z'),
                    ],
                );
            },
            new RunLimits(stopOnIdle: true),
            CommitPolicy::None,
            $narrate,
        );

        if (null !== $producer && $replayed > 0) {
            $producer->flush();
            if ($producer->failedDeliveries() > 0) {
                $output->writeln(sprintf('<error>%d replay deliveries were not acked — re-run the replay; duplicates are absorbed by event-id dedup.</error>', $producer->failedDeliveries()));

                return Command::FAILURE;
            }
        }

        $output->writeln('');
        $output->writeln(0 === $replayed && 0 === $skipped
            ? '<info>DLQ is empty</info> — nothing to replay'
            : sprintf('<info>done</info> — %s %d message(s)%s. The DLQ itself is untouched (it is the audit trail).', $dryRun ? 'would replay' : 'replayed', $replayed, $skipped > 0 ? sprintf(', %d unroutable', $skipped) : ''));

        return Command::SUCCESS;
    }

    /**
     * The original headers minus the per-hop diagnostics — the replayed message
     * starts fresh; only its identity (message-name, event-id) must survive.
     *
     * @return array<string, string>
     */
    private function replayHeaders(\RdKafka\Message $message): array
    {
        $headers = [];
        foreach ($message->headers as $key => $value) {
            if (! in_array($key, self::DIAGNOSTICS, true)) {
                $headers[$key] = $value;
            }
        }

        return $headers;
    }

    private function header(\RdKafka\Message $message, string $key, string $fallback = ''): string
    {
        $value = $message->headers[$key] ?? null;

        return is_string($value) && '' !== $value ? $value : $fallback;
    }

    /**
     * The runtime extension types the key ?string (null for unkeyed records),
     * whatever the stubs claim — this seam owns the nullability honestly.
     */
    private function keyLabel(?string $key): string
    {
        return $key ?? '<none>';
    }
}
