<?php

declare(strict_types=1);

namespace Workshop\Console;

use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Workshop\Kernel\AvroEventSerializer;
use Workshop\Kernel\KafkaContextFactory;

#[AsCommand(
    name: 'events:consume',
    description: 'Consume a topic, AVRO-decode each message via Schema Registry, and print the envelope (metadata + payload).',
)]
final class EventConsumeCommand extends Command
{
    public function __construct(
        private readonly KafkaContextFactory $kafka,
        private readonly AvroEventSerializer $avro,
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
        $topic = (string) $input->getArgument('topic');
        $max = (int) $input->getOption('max');
        $timeoutMs = (int) $input->getOption('timeout');
        $group = null !== $input->getOption('group')
            ? (string) $input->getOption('group')
            : sprintf('ephemeral-%s-%d', $topic, getmypid());

        $context = $this->kafka->forConsumer($group);
        $consumer = $context->createConsumer($context->createTopic($topic));
        $consumer->setCommitAsync(false);

        $received = 0;
        while (0 === $max || $received < $max) {
            $message = $consumer->receive($timeoutMs);
            if (null === $message) {
                break;
            }

            $event = $this->avro->decode($message->getBody());
            $kafkaMessage = $message->getKafkaMessage();
            $metadata = $event['metadata'] ?? [];
            $payload = $event;
            unset($payload['metadata']);

            $output->writeln(sprintf('── partition=%d offset=%d key=%s', $kafkaMessage->partition, $kafkaMessage->offset, $message->getKey() ?? '<null>'));
            $output->writeln('metadata ' . $this->pretty($metadata));
            $output->writeln('payload  ' . $this->pretty($payload));

            $consumer->acknowledge($message);
            ++$received;
        }

        $context->close();

        return Command::SUCCESS;
    }

    private function pretty(mixed $value): string
    {
        return (string) json_encode($value, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);
    }
}
