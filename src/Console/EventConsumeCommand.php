<?php

declare(strict_types=1);

namespace Workshop\Console;

use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Workshop\Kafka\Client\ConsumerFactory;
use Workshop\Kafka\Runtime\CommitPolicy;
use Workshop\Kafka\Runtime\ConsumerRunner;
use Workshop\Kafka\Runtime\RunLimits;
use Workshop\Kafka\Serde\AvroEnvelopeSerializer;

#[AsCommand(
    name: 'events:consume',
    description: 'Consume a topic, AVRO-decode each message via Schema Registry, and print the envelope (metadata + payload).',
)]
final class EventConsumeCommand extends Command
{
    public function __construct(
        private readonly ConsumerFactory $consumers,
        private readonly ConsumerRunner $runner,
        private readonly AvroEnvelopeSerializer $avro,
    ) {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this
            ->addArgument('topic', InputArgument::REQUIRED, 'Topic to consume (e.g. enet.ecommerce.orders)')
            ->addOption('group', 'g', InputOption::VALUE_REQUIRED, 'Consumer group; omit for an ephemeral group from earliest')
            ->addOption('max', 'm', InputOption::VALUE_REQUIRED, 'Stop after this many messages (0 = until the receive timeout)', 0)
            ->addOption('timeout', 't', InputOption::VALUE_REQUIRED, 'Receive timeout in ms', 5000);
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $topic = Input::string($input, 'topic');
        $max = Input::int($input, 'max');
        $timeoutMs = Input::int($input, 'timeout');
        $groupOption = Input::stringOrNull($input, 'group');
        $named = null !== $groupOption;
        $group = $groupOption ?? sprintf('ephemeral-%s-%d', $topic, getmypid());

        $consumer = $this->consumers->create($named ? 'consumer.at-least-once' : 'consumer.ephemeral', $group);

        $handler = function (\RdKafka\Message $message) use ($output): void {
            $output->writeln(sprintf('── partition=%d offset=%d key=%s', $message->partition, $message->offset, $message->key ?? '<null>'));

            $event = $this->avro->decode($message->payload);
            if (null === $event) {
                $output->writeln('  (non-AVRO bytes, skipped)');

                return;
            }

            $metadata = $event['metadata'] ?? [];
            $payload = $event;
            unset($payload['metadata']);

            $output->writeln('metadata ' . $this->pretty($metadata));
            $output->writeln('payload  ' . $this->pretty($payload));
        };

        $this->runner->run(
            $consumer,
            [$topic],
            $handler,
            new RunLimits(maxMessages: $max, pollTimeoutMs: $timeoutMs, stopOnIdle: true),
            CommitPolicy::AfterEachMessage,
        );

        return Command::SUCCESS;
    }

    private function pretty(mixed $value): string
    {
        return (string) json_encode($value, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);
    }
}
