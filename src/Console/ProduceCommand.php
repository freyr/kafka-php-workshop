<?php

declare(strict_types=1);

namespace Workshop\Console;

use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Workshop\Kafka\Client\ProducerFactory;
use Workshop\Kafka\Serde\JsonSerializer;
use Workshop\Produce\TextMessage;

#[AsCommand(
    name: 'produce',
    description: 'Produce N TextMessages to the routed json topic. Cycle through --key for keyed partitioning, or --unkeyed to scatter them.',
)]
final class ProduceCommand extends Command
{
    public function __construct(
        private readonly ProducerFactory $producers,
        private readonly JsonSerializer $serializer,
    ) {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this
            ->addOption('count', 'c', InputOption::VALUE_REQUIRED, 'Number of messages to produce', 10)
            ->addOption('key', 'k', InputOption::VALUE_REQUIRED, 'Comma-separated semantic keys; messages cycle through them (crc32(key) routes each key to a fixed partition). Omit for unkeyed: a random partition per message')
            ->addOption('key-cardinality', null, InputOption::VALUE_REQUIRED, 'Generate this many distinct synthetic keys (key-0..key-{N-1}) and cycle through them; shows even spread from a high-cardinality key. Mutually exclusive with --key')
            ->addOption('unkeyed', null, InputOption::VALUE_NONE, 'Scatter every message across partitions, ignoring keys (overrides --key)')
            ->addOption('payload', null, InputOption::VALUE_REQUIRED, 'Payload template; {n} = 1-based index, {key} = message key', 'event-{n}')
            ->addOption('profile', null, InputOption::VALUE_REQUIRED, 'Producer profile to build the client from', 'producer.simple');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $count = Input::int($input, 'count');
        $template = Input::string($input, 'payload');
        $profile = Input::string($input, 'profile');
        $forceUnkeyed = (bool) $input->getOption('unkeyed');

        $cardinality = Input::intOrNull($input, 'key-cardinality');
        $keys = $this->parseKeys(Input::stringOrNull($input, 'key'));

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
            $text = strtr($template, [
                '{n}' => (string) $i,
                '{key}' => $key ?? '',
            ]);
            $message = TextMessage::create($i, $key, $text);

            $unkeyed = $forceUnkeyed || null === $key;
            $produced = $producer->produce($message, $unkeyed);

            $output->writeln($this->describe($produced->route->topic, $unkeyed ? null : $key, $text));
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

    private function describe(string $topic, ?string $key, string $payload): string
    {
        $parts = ["topic={$topic}"];
        $parts[] = null !== $key ? "key={$key}" : 'unkeyed';
        $parts[] = "value={$payload}";

        return 'produced: ' . implode(' ', $parts);
    }
}
