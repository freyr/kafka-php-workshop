<?php

declare(strict_types=1);

namespace Workshop\App\Console;

use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use Workshop\Kafka\Client\ConsumerFactory;
use Workshop\Kafka\Runtime\CommitPolicy;
use Workshop\Kafka\Runtime\OffsetReset;
use Workshop\Kafka\Runtime\RunLimits;

#[AsCommand(
    name: 'kafka:dlq:inspect',
    description: 'Read a DLQ topic and print each message\'s diagnostic headers — the triage view. Read-only: an ephemeral group, never commits, never re-produces.',
)]
final class DlqInspectCommand extends Command
{
    /**
     * The diagnostic headers the ErrorRouter stamps, in display order.
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
    ) {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this->addArgument('topic', InputArgument::OPTIONAL, 'DLQ topic to inspect', 'enet.ecommerce.outbox.ErrorDemo.dlq');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $topic = Input::string($input, 'topic');

        $narrate = $output->isVerbose()
            ? static fn (string $line) => $output->writeln('  <comment>' . $line . '</comment>')
            : null;

        // Same shape as the ephemeral consume lane: a fresh throwaway group from
        // the beginning, drained to the first empty poll, nothing committed.
        $consumer = $this->consumers->create(
            'consumer.ephemeral',
            sprintf('dlq-inspect-%s-%d-%d', $topic, getmypid(), time()),
            OffsetReset::Beginning,
            $narrate,
        );

        $output->writeln(sprintf('<comment>inspecting %s (read-only)</comment>', $topic));

        $seen = 0;
        $consumer->run(
            [$topic],
            function (\RdKafka\Message $message) use ($output, &$seen): void {
                ++$seen;
                $output->writeln('');
                $output->writeln(sprintf(
                    '<info>[%d]</info> %s id=%s key=%s partition=%d offset=%d (%d payload bytes)',
                    $seen,
                    $this->header($message, 'message-name', '<none>'),
                    $this->header($message, 'event-id', '<none>'),
                    $this->keyLabel($message->key),
                    $message->partition,
                    $message->offset,
                    \strlen((string) $message->payload),
                ));
                foreach (self::DIAGNOSTICS as $name) {
                    $output->writeln(sprintf('      %-22s = <comment>%s</comment>', $name, $this->header($message, $name, '—')));
                }
            },
            new RunLimits(stopOnIdle: true),
            CommitPolicy::None,
            $narrate,
        );

        $output->writeln('');
        $output->writeln(0 === $seen
            ? '<info>DLQ is empty</info> — nothing to triage'
            : sprintf('<info>done</info> — %d dead-lettered message(s). Fix the cause, then replay with <comment>kafka:dlq:replay</comment> (idempotency makes replay safe to re-run).', $seen));

        return Command::SUCCESS;
    }

    private function header(\RdKafka\Message $message, string $key, string $fallback): string
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
