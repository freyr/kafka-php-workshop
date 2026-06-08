<?php

declare(strict_types=1);

namespace Workshop\Console;

use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Workshop\Kafka\Callback\CallbackKit;
use Workshop\Kafka\Callback\ErrorCallback;
use Workshop\Kafka\Callback\RebalanceCallback;
use Workshop\Kafka\Client\ConsumerFactory;
use Workshop\Kafka\Runtime\CommitPolicy;
use Workshop\Kafka\Runtime\ConsumerRunner;
use Workshop\Kafka\Runtime\RunLimits;
use Workshop\Kafka\Serde\MessageSerializer;

#[AsCommand(
    name: 'consume',
    description: 'Consume a topic under a consumer group, committing offsets. Omit --group to read from earliest under a throwaway group.',
)]
final class ConsumeCommand extends Command
{
    public function __construct(
        private readonly ConsumerFactory $consumers,
        private readonly ConsumerRunner $runner,
        private readonly MessageSerializer $serializer,
    ) {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this
            ->addArgument('topic', InputArgument::REQUIRED, 'Topic to consume from (e.g. consumer-groups-events)')
            ->addOption('group', 'g', InputOption::VALUE_REQUIRED, 'Consumer group ID; omit for an ephemeral group that always starts from earliest')
            ->addOption('max', 'm', InputOption::VALUE_REQUIRED, 'Stop after this many messages (0 = read until the receive timeout)', 0)
            ->addOption('timeout', 't', InputOption::VALUE_REQUIRED, 'Receive timeout in ms; a poll returning nothing within it ends the run', 5000)
            ->addOption('no-commit', null, InputOption::VALUE_NONE, 'Do not commit offsets (read-only tail)');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $topicName = Input::string($input, 'topic');
        $max = Input::int($input, 'max');
        $timeoutMs = Input::int($input, 'timeout');
        $commit = ! (bool) $input->getOption('no-commit');
        $groupOption = Input::stringOrNull($input, 'group');
        $named = null !== $groupOption;
        $group = $this->resolveGroup($groupOption, $topicName);

        $output->writeln(sprintf('<comment>group=%s commit=%s</comment>', $group, $commit ? 'yes' : 'no'));

        // A named group is a real, reusable consumer (static membership + commit);
        // an omitted group is a throwaway that always replays from earliest.
        $profile = $named ? 'consumer.at-least-once' : 'consumer.ephemeral';
        $policy = $commit ? CommitPolicy::AfterEachMessage : CommitPolicy::None;

        $narrate = $output->isVerbose()
            ? function (string $line) use ($output): void {
                $output->writeln('  <comment>' . $line . '</comment>');
            }
        : null;

        $consumer = $this->consumers->create($profile, $group, $this->callbacks($narrate));

        $handler = function (\RdKafka\Message $message) use ($output): void {
            $output->writeln(sprintf(
                'partition=%d offset=%d key=%s value=%s',
                $message->partition,
                $message->offset,
                $message->key ?? '<null>',
                $this->render($message->payload),
            ));
        };

        $this->runner->run(
            $consumer,
            [$topicName],
            $handler,
            new RunLimits(maxMessages: $max, pollTimeoutMs: $timeoutMs, stopOnIdle: true),
            $policy,
            $narrate,
        );

        return Command::SUCCESS;
    }

    /**
     * Render a consumed payload by decoding it back to its fields; anything the
     * serializer cannot decode (a non-AVRO record on the topic) is shown verbatim
     * rather than crashing the run.
     */
    private function render(?string $payload): string
    {
        if (null === $payload) {
            return '<null>';
        }

        try {
            $decoded = $this->serializer->decode($payload);
        } catch (\Throwable) {
            return $payload;
        }

        if (! is_array($decoded)) {
            return $payload;
        }

        $fields = [];
        foreach ($decoded as $field => $value) {
            $fields[] = sprintf('%s: %s', $field, $this->stringify($value));
        }

        return '{' . implode(', ', $fields) . '}';
    }

    private function stringify(mixed $value): string
    {
        return match (true) {
            null === $value => 'null',
            is_bool($value) => $value ? 'true' : 'false',
            is_scalar($value) => (string) $value,
            default => json_encode($value) ?: '<unrenderable>',
        };
    }

    private function callbacks(?\Closure $narrate): ?CallbackKit
    {
        if (null === $narrate) {
            return null; // factory default kit (silent rebalance + error)
        }

        return new CallbackKit(new RebalanceCallback($narrate), new ErrorCallback($narrate));
    }

    /**
     * A named group keeps its committed offsets across runs (real consumer-group
     * behaviour); an ephemeral, never-reused id forces every run to start at the
     * earliest offset — handy for inspecting a topic's full contents.
     */
    private function resolveGroup(?string $group, string $topicName): string
    {
        if (null !== $group) {
            return $group;
        }

        return sprintf('ephemeral-%s-%d-%d', $topicName, getmypid(), time());
    }
}
