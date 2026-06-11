<?php

declare(strict_types=1);

namespace Workshop\App\Console;

use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Workshop\App\Consumer\DlqRepair;
use Workshop\Kafka\Client\ConsumerFactory;
use Workshop\Kafka\Client\ProducerFactory;
use Workshop\Kafka\Runtime\CommitPolicy;
use Workshop\Kafka\Runtime\OffsetReset;
use Workshop\Kafka\Runtime\RunLimits;

#[AsCommand(
    name: 'kafka:dlq:replay',
    description: 'The DLQ recovery step: repair selected dead letters (restore a missing header, re-frame raw AVRO) and re-publish them to their x-original-topic. Manual, per-message work by design — the DLQ holds messages broken in themselves.',
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
        private readonly DlqRepair $repair,
    ) {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this
            ->addArgument('topic', InputArgument::OPTIONAL, 'DLQ topic to replay from', 'enet.ecommerce.outbox.ErrorDemo.dlq')
            ->addOption('dry-run', null, InputOption::VALUE_NONE, 'Show what would be repaired and replayed where, produce nothing')
            ->addOption('id', null, InputOption::VALUE_REQUIRED | InputOption::VALUE_IS_ARRAY, 'Replay only the message(s) with these event-id header values — the per-message triage decision. Unselected messages stay in the DLQ ("drop" = leave them to retention). Omit to process every message')
            ->addOption('fix-frame', null, InputOption::VALUE_NONE, 'Repair: re-frame a payload that shipped as raw AVRO without the Confluent frame, stamping the subject\'s latest registered schema id (the subject resolves from the message-name)')
            ->addOption('fix-message-name', null, InputOption::VALUE_REQUIRED, 'Repair: restore a missing message-name header with this value — nothing in the bytes can supply it, only the operator\'s diagnosis');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $topic = Input::string($input, 'topic');
        $dryRun = (bool) $input->getOption('dry-run');
        $fixFrame = (bool) $input->getOption('fix-frame');
        $fixMessageName = Input::stringOrNull($input, 'fix-message-name');
        $onlyIds = $this->ids($input);

        $narrate = $output->isVerbose()
            ? static fn (string $line) => $output->writeln('  <comment>' . $line . '</comment>')
            : null;

        $producer = $dryRun ? null : $this->producers->createRaw('producer.dlq', $narrate);

        // The DLQ is read with a throwaway group and never committed: replay
        // leaves the DLQ untouched (it is the append-only audit trail — Kafka
        // cannot edit or delete individual records anyway), and re-running is
        // harmless because the consumer dedups on event-id.
        $consumer = $this->consumers->create(
            'consumer.ephemeral',
            sprintf('dlq-replay-%s-%d-%d', $topic, getmypid(), time()),
            OffsetReset::Beginning,
            $narrate,
        );

        $output->writeln(sprintf(
            '<comment>replaying %s%s%s</comment>',
            $topic,
            [] !== $onlyIds ? sprintf(' (only %d selected id(s))', count($onlyIds)) : '',
            $dryRun ? ' — dry run, nothing is produced' : '',
        ));

        $replayed = 0;
        $skipped = 0;
        $left = 0;
        $consumer->run(
            [$topic],
            function (\RdKafka\Message $message) use ($producer, $dryRun, $fixFrame, $fixMessageName, $onlyIds, $output, &$replayed, &$skipped, &$left): void {
                $eventId = $this->header($message, 'event-id');

                // The per-message triage decision: an unselected message is the
                // "drop" outcome — it stays in the DLQ until retention ages it out.
                if ([] !== $onlyIds && ! in_array($eventId, $onlyIds, true)) {
                    ++$left;

                    return;
                }

                $destination = $this->header($message, 'x-original-topic');
                if ('' === $destination) {
                    ++$skipped;
                    $output->writeln(sprintf('  <error>✗</error> offset=%d has no x-original-topic header — cannot route, leaving it in the DLQ', $message->offset));

                    return;
                }

                $repaired = $this->repair->repair(
                    (string) $message->payload,
                    $this->replayHeaders($message),
                    $fixFrame,
                    $fixMessageName,
                );

                $output->writeln(sprintf(
                    '  %s %s id=%s → <comment>%s</comment> (key=%s preserved)',
                    $dryRun ? '<comment>would replay</comment>' : '<info>↩ replayed</info>',
                    '' !== ($repaired['headers']['message-name'] ?? '') ? $repaired['headers']['message-name'] : '<none>',
                    '' !== $eventId ? $eventId : '<none>',
                    $destination,
                    $this->keyLabel($message->key),
                ));
                foreach ($repaired['applied'] as $fix) {
                    $output->writeln(sprintf('      <info>🔧 %s</info>', $fix));
                }
                foreach ($repaired['defects'] as $defect) {
                    $output->writeln(sprintf('      <fg=yellow>⚠ still broken: %s — replaying it will just poison it again</>', $defect));
                }
                ++$replayed;

                if ($dryRun || null === $producer) {
                    return;
                }

                $producer->produce(
                    $destination,
                    $message->key,
                    $repaired['payload'],
                    [
                        ...$repaired['headers'],
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
        $output->writeln(0 === $replayed && 0 === $skipped && 0 === $left
            ? '<info>DLQ is empty</info> — nothing to replay'
            : sprintf(
                '<info>done</info> — %s %d message(s)%s%s. The DLQ itself is untouched (append-only audit trail).',
                $dryRun ? 'would replay' : 'replayed',
                $replayed,
                $left > 0 ? sprintf(', left %d unselected (drop = retention ages them out)', $left) : '',
                $skipped > 0 ? sprintf(', %d unroutable', $skipped) : '',
            ));

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

    /**
     * @return list<string>
     */
    private function ids(InputInterface $input): array
    {
        $raw = $input->getOption('id');
        if (! is_array($raw)) {
            return [];
        }

        return array_values(array_filter(array_map(static fn (mixed $id): string => is_scalar($id) ? (string) $id : '', $raw), static fn (string $id): bool => '' !== $id));
    }

    private function header(\RdKafka\Message $message, string $key): string
    {
        $value = $message->headers[$key] ?? null;

        return is_string($value) ? $value : '';
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
