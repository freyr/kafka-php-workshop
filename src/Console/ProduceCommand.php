<?php

declare(strict_types=1);

namespace Workshop\Console;

use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Workshop\Kafka\Client\ProducerFactory;
use Workshop\Kafka\Serde\StringSerializer;

#[AsCommand(
    name: 'produce',
    description: 'Produce N messages to a topic. Cycle through --key for keyed routing, or pin every message to a --partition.',
)]
final class ProduceCommand extends Command
{
    use InputCasts;

    public function __construct(
        private readonly ProducerFactory $producers,
        private readonly StringSerializer $serializer,
    ) {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this
            ->addArgument('topic', InputArgument::REQUIRED, 'Topic to produce to (e.g. consumer-groups-events)')
            ->addOption('count', 'c', InputOption::VALUE_REQUIRED, 'Number of messages to produce', 10)
            ->addOption('key', 'k', InputOption::VALUE_REQUIRED, 'Comma-separated semantic keys; messages cycle through them (crc32(key) routes each key to a fixed partition). Omit for unkeyed: a random partition per message')
            ->addOption('key-cardinality', null, InputOption::VALUE_REQUIRED, 'Generate this many distinct synthetic keys (key-0..key-{N-1}) and cycle through them; shows even spread from a high-cardinality key. Mutually exclusive with --key')
            ->addOption('partition', 'p', InputOption::VALUE_REQUIRED, 'Pin every message to this partition (overrides key-based routing)')
            ->addOption('payload', null, InputOption::VALUE_REQUIRED, 'Payload template; {n} = 1-based index, {key} = message key', 'event-{n}')
            ->addOption('profile', null, InputOption::VALUE_REQUIRED, 'Producer profile to build the client from', 'producer.simple');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $topicName = $this->argString($input, 'topic');
        $count = $this->optInt($input, 'count');
        $partition = $this->optIntOrNull($input, 'partition');
        $template = $this->optString($input, 'payload') ?? 'event-{n}';
        $profile = $this->optString($input, 'profile') ?? 'producer.simple';

        $cardinality = $this->optIntOrNull($input, 'key-cardinality');
        $keys = $this->parseKeys($this->optString($input, 'key'));

        if (null !== $cardinality && [] !== $keys) {
            $output->writeln('<error>--key and --key-cardinality are mutually exclusive.</error>');

            return Command::INVALID;
        }
        if (null !== $cardinality) {
            if ($cardinality < 1) {
                $output->writeln('<error>--key-cardinality must be >= 1.</error>');

                return Command::INVALID;
            }
            $keys = array_map(static fn (int $i): string => 'key-' . $i, range(0, $cardinality - 1));
        }

        $producer = $this->producers->create($profile, $this->serializer);

        for ($i = 1; $i <= $count; ++$i) {
            $key = [] === $keys ? null : $keys[($i - 1) % count($keys)];
            $payload = strtr($template, [
                '{n}' => (string) $i,
                '{key}' => $key ?? '',
            ]);

            if (null !== $partition) {
                $producer->toPartition($topicName, $partition, $payload, $key);
            } elseif (null !== $key) {
                $producer->keyed($topicName, $key, $payload);
            } else {
                $producer->unkeyed($topicName, $payload);
            }

            $output->writeln($this->describe($partition, $key, $payload));
        }

        $producer->close();

        return Command::SUCCESS;
    }

    /**
     * @return list<string>
     */
    private function parseKeys(?string $raw): array
    {
        if (null === $raw) {
            return [];
        }

        return array_values(array_filter(
            array_map('trim', explode(',', $raw)),
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
