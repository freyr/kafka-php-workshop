<?php

declare(strict_types=1);

namespace Workshop\Console;

use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Workshop\Kernel\KafkaContextFactory;

#[AsCommand(
    name: 'produce',
    description: 'Produce N messages to a topic. Cycle through --key for keyed routing, or pin every message to a --partition.',
)]
final class ProduceCommand extends Command
{
    public function __construct(
        private readonly KafkaContextFactory $kafka,
    ) {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this
            ->addArgument('topic', InputArgument::REQUIRED, 'Topic to produce to (e.g. consumer-groups-events)')
            ->addOption('count', 'c', InputOption::VALUE_REQUIRED, 'Number of messages to produce', 10)
            ->addOption('key', 'k', InputOption::VALUE_REQUIRED, 'Comma-separated semantic keys; messages cycle through them (crc32(key) routes each key to a fixed partition). Omit for unkeyed: a random partition per message')
            ->addOption('partition', 'p', InputOption::VALUE_REQUIRED, 'Pin every message to this partition (overrides key-based routing)')
            ->addOption('payload', null, InputOption::VALUE_REQUIRED, 'Payload template; {n} = 1-based index, {key} = message key', 'event-{n}');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $topicName = (string) $input->getArgument('topic');
        $count = (int) $input->getOption('count');
        $keys = $this->parseKeys($input->getOption('key'));
        $partition = null === $input->getOption('partition') ? null : (int) $input->getOption('partition');
        $template = (string) $input->getOption('payload');

        $context = $this->kafka->forProducer();
        $topic = $context->createTopic($topicName);
        $producer = $context->createProducer();

        for ($i = 1; $i <= $count; ++$i) {
            $key = [] === $keys ? null : $keys[($i - 1) % count($keys)];
            $payload = strtr($template, [
                '{n}' => (string) $i,
                '{key}' => $key ?? '',
            ]);

            $message = $context->createMessage($payload);
            if (null !== $key) {
                $message->setKey($key);
            }
            if (null !== $partition) {
                $message->setPartition($partition);
            }
            $producer->send($topic, $message);

            $output->writeln($this->describe($partition, $key, $payload));
        }

        $context->close();

        return Command::SUCCESS;
    }

    /**
     * @return list<string>
     */
    private function parseKeys(mixed $raw): array
    {
        if (null === $raw) {
            return [];
        }

        return array_values(array_filter(
            array_map('trim', explode(',', (string) $raw)),
            static fn (string $key): bool => '' !== $key,
        ));
    }

    private function describe(?int $partition, ?string $key, string $payload): string
    {
        $parts = [];
        if (null !== $partition) {
            $parts[] = "partition={$partition}";
        }
        if (null !== $key) {
            $parts[] = "key={$key}";
        }
        $parts[] = "value={$payload}";

        return 'produced: ' . implode(' ', $parts);
    }
}
