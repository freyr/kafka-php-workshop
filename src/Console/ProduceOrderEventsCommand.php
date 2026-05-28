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
    name: 'consumer-groups:produce',
    description: 'Produce order-placed events to the consumer-groups topic.',
)]
final class ProduceOrderEventsCommand extends Command
{
    public function __construct(
        private readonly KafkaContextFactory $kafka,
    ) {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this->addOption('count', 'c', InputOption::VALUE_REQUIRED, 'Number of messages to produce', 5);
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $count = (int) $input->getOption('count');

        $context = $this->kafka->forProducer();
        $topic = $context->createTopic(Topics::ConsumerGroups->value);
        $producer = $context->createProducer();

        for ($i = 1; $i <= $count; ++$i) {
            $payload = "order-placed-{$i}";
            $producer->send($topic, $context->createMessage($payload));
            $output->writeln("produced: {$payload}");
        }

        $context->close();

        return Command::SUCCESS;
    }
}
