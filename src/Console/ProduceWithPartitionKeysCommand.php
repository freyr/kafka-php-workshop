<?php

declare(strict_types=1);

namespace Workshop\Console;

use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Workshop\Kernel\KafkaContextFactory;
use Workshop\Kernel\Topics;

#[AsCommand(
    name: 'partitioning:produce',
    description: 'Produce events to the partitioning topic; --keyed cycles through a key list to demonstrate stable key->partition mapping.',
)]
final class ProduceWithPartitionKeysCommand extends Command
{
    private const array DEFAULT_KEYS = ['alice', 'bob', 'carol', 'dave'];

    public function __construct(
        private readonly KafkaContextFactory $kafka,
    ) {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this
            ->addOption('count', 'c', InputOption::VALUE_REQUIRED, 'Number of messages to produce', 20)
            ->addOption('keyed', 'k', InputOption::VALUE_NONE, 'Produce keyed messages (cycle through --keys)')
            ->addOption('keys', null, InputOption::VALUE_REQUIRED, 'Comma-separated key list when --keyed is set', implode(',', self::DEFAULT_KEYS));
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $count = (int) $input->getOption('count');
        $keyed = (bool) $input->getOption('keyed');
        $keys = $keyed
            ? array_values(array_filter(array_map('trim', explode(',', (string) $input->getOption('keys')))))
            : [];

        if ($keyed && [] === $keys) {
            $output->writeln('<error>--keyed requires --keys with at least one entry.</error>');

            return Command::INVALID;
        }

        $context = $this->kafka->forProducer();
        $topic = $context->createTopic(Topics::Partitioning->value);
        $producer = $context->createProducer();

        for ($i = 1; $i <= $count; ++$i) {
            if ($keyed) {
                $key = $keys[($i - 1) % count($keys)];
                $payload = "event-{$i}";
                $message = $context->createMessage($payload);
                $message->setKey($key);
                $producer->send($topic, $message);
                $output->writeln("produced: key={$key} value={$payload}");
            } else {
                $payload = "unkeyed-event-{$i}";
                $producer->send($topic, $context->createMessage($payload));
                $output->writeln("produced: {$payload}");
            }
        }

        $context->close();

        return Command::SUCCESS;
    }
}
