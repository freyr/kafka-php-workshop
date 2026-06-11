<?php

declare(strict_types=1);

namespace Workshop\Enqueue\Console;

use FlixTech\SchemaRegistryApi\Exception\SchemaRegistryException;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Uid\Uuid;
use Workshop\App\Console\Input;
use Workshop\App\Producer\MessageCatalog;
use Workshop\App\Producer\MessageRouting;
use Workshop\Enqueue\EnqueueContextFactory;
use Workshop\Kafka\Serde\MessageSerializer;

#[AsCommand(
    name: 'enqueue:produce',
    description: 'Simulate php-fpm requests producing one message each through enqueue — delivery report confirmed before every "response".',
)]
final class EnqueueProduceCommand extends Command
{
    private const int FLUSH_TIMEOUT_MS = 10000;

    public function __construct(
        private readonly EnqueueContextFactory $contexts,
        private readonly MessageCatalog $catalog,
        private readonly MessageRouting $routing,
        private readonly MessageSerializer $serializer,
    ) {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this
            ->addOption('count', 'c', InputOption::VALUE_REQUIRED, 'How many simulated requests (one message each); default: 1', '1')
            ->addOption('message-name', null, InputOption::VALUE_REQUIRED, 'Produce only this message (e.g. order.created); omit to pick a random message per request');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $count = Input::int($input, 'count');
        $pin = Input::stringOrNull($input, 'message-name');

        if ($count < 1) {
            $output->writeln('<error>--count must be >= 1.</error>');

            return Command::INVALID;
        }
        if (null !== $pin && ! $this->catalog->has($pin)) {
            $output->writeln(sprintf('<error>Unknown message name: %s</error>', $pin));
            $output->writeln('Available: ' . implode(', ', $this->catalog->names()));

            return Command::INVALID;
        }

        $names = null !== $pin ? [$pin] : $this->catalog->names();

        // One producer for the whole run — the php-fpm reality: librdkafka lives
        // per worker PROCESS and is reused across the requests that worker serves.
        // What is per-request is the send + flush cycle below, never the client.
        $context = $this->contexts->fpmProducer();
        $producer = $context->createProducer();

        $output->writeln(sprintf('<comment>fpm-style producer — %d request(s), acks=all, max.in.flight=1, flush before each response</comment>', $count));

        try {
            for ($request = 1; $request <= $count; ++$request) {
                // Each simulated request handles an unrelated order — fresh id, no pool.
                $name = $names[array_rand($names)];
                $message = $this->catalog->build($name, 'ord-' . substr(Uuid::v4()->toRfc4122(), 0, 8));

                $record = $context->createMessage($this->serializer->encode($message), [], [
                    'message-name' => $name,
                    'event-id' => $message->eventId(),
                ]);
                $record->setKey($message->partitionKey());

                $producer->send($context->createTopic($this->routing->for($name)->topic), $record);

                // The request's contract: block until the broker acked (or the
                // bounded message.timeout failed) every byte this request produced.
                // A flush that cannot drain crashes the request — losing the buffer
                // silently on worker shutdown is the one unacceptable outcome.
                $flushResult = $producer->flush(self::FLUSH_TIMEOUT_MS);
                if (null !== $flushResult && RD_KAFKA_RESP_ERR_NO_ERROR !== $flushResult) {
                    throw new \RuntimeException(sprintf('Producer flush did not drain within %dms (request #%d) — the message may be undelivered, refusing to "respond".', self::FLUSH_TIMEOUT_MS, $request));
                }

                $output->writeln(sprintf(
                    'request #%d: <info>%s</info> → %s key=%s <comment>(delivery confirmed)</comment>',
                    $request,
                    $name,
                    $this->routing->for($name)->topic,
                    $message->partitionKey(),
                ));
            }
        } catch (SchemaRegistryException) {
            $output->writeln('<error>No schema registered for this event.</error>');
            $output->writeln('Schemas are not auto-registered — register them first, then produce again:');
            $output->writeln('  <comment>bin/console kafka:schema:register --all</comment>');

            return Command::FAILURE;
        }

        $output->writeln(sprintf('<info>done</info> — %d request(s) served, every message broker-acked before its response', $count));

        return Command::SUCCESS;
    }
}
