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
    name: 'offsets:consume',
    description: 'Drain the offsets topic under the fixed offsets-group; exits on receive timeout.',
)]
final class ConsumeAndCommitOffsetsCommand extends Command
{
    private const string GROUP = 'offsets-group';

    public function __construct(
        private readonly KafkaContextFactory $kafka,
    ) {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this->addOption('timeout', 't', InputOption::VALUE_REQUIRED, 'Receive timeout in ms', 5000);
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $timeoutMs = (int) $input->getOption('timeout');

        $context = $this->kafka->forConsumer(self::GROUP);
        $consumer = $context->createConsumer($context->createTopic(Topics::Offsets->value));
        $consumer->setCommitAsync(false);

        while (true) {
            $message = $consumer->receive($timeoutMs);
            if (null === $message) {
                break;
            }

            $kafkaMessage = $message->getKafkaMessage();
            $output->writeln(sprintf(
                'partition=%d offset=%d value=%s',
                $kafkaMessage->partition,
                $kafkaMessage->offset,
                $message->getBody(),
            ));

            $consumer->acknowledge($message);
        }

        $context->close();

        return Command::SUCCESS;
    }
}
