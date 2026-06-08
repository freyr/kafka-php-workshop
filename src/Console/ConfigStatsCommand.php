<?php

declare(strict_types=1);

namespace Workshop\Console;

use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Workshop\Kafka\Config\KafkaTuning;

#[AsCommand(
    name: 'kafka:config:stats',
    description: 'RAW php-rdkafka consumer that reads consumer lag, broker RTT, and fetch-queue depth from the librdkafka statistics callback.',
)]
final class ConfigStatsCommand extends Command
{
    public function __construct(
        private readonly string $brokers,
        private readonly KafkaTuning $tuning,
    ) {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this
            ->addArgument('topic', InputArgument::OPTIONAL, 'Topic to consume', 'enet.ecommerce.orders')
            ->addOption('group', 'g', InputOption::VALUE_REQUIRED, 'Consumer group; omit for an ephemeral group that reads from earliest so the backlog (and its lag) is visible')
            ->addOption('runtime', 'r', InputOption::VALUE_REQUIRED, 'Run for this many seconds, then shut down gracefully (0 = until --max or a signal)', 10)
            ->addOption('max', 'm', InputOption::VALUE_REQUIRED, 'Stop after this many messages (0 = no message cap)', 0)
            ->addOption('timeout', 't', InputOption::VALUE_REQUIRED, 'consume() poll timeout in ms', 1000)
            ->addOption('stats-interval', null, InputOption::VALUE_REQUIRED, 'librdkafka statistics emission interval in ms (production uses 10000; lower for a livelier demo)', 2000)
            ->addOption('slow', null, InputOption::VALUE_REQUIRED, 'Sleep this many ms per message to simulate slow processing — makes lag build up and visibly drain in the stats output (also how you blow max.poll.interval.ms)', 0);
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        if (! class_exists(\RdKafka\Conf::class)) {
            $output->writeln('<error>php-rdkafka extension not loaded.</error>');

            return Command::FAILURE;
        }

        $topic = Input::string($input, 'topic');
        $group = $this->resolveGroup(Input::stringOrNull($input, 'group'), $topic);
        $runtime = Input::int($input, 'runtime');
        $max = Input::int($input, 'max');
        $timeoutMs = Input::int($input, 'timeout');
        $statsInterval = Input::int($input, 'stats-interval');
        $slowMs = max(0, Input::int($input, 'slow'));

        // Graceful shutdown — flip $running and let the loop fall through to the
        // raw commit() + close() sequence. close() sends LeaveGroup immediately,
        // so the group rebalances now instead of waiting out session.timeout.ms.
        $running = true;
        pcntl_async_signals(true);
        $stop = function (int $signal) use (&$running, $output): void {
            $output->writeln(sprintf('<comment>received %s — committing and leaving the group…</comment>', SIGTERM === $signal ? 'SIGTERM' : 'SIGINT'));
            $running = false;
        };
        pcntl_signal(SIGTERM, $stop);
        pcntl_signal(SIGINT, $stop);

        $conf = new \RdKafka\Conf();
        $conf->set('metadata.broker.list', $this->brokers);
        $conf->set('group.id', $group);

        // The whole recommended consumer config from the one source of truth.
        foreach ($this->tuning->consumerOverrides() as $key => $value) {
            $conf->set($key, $value);
        }
        // A throwaway demo must not get fenced by static membership on a re-run,
        // so make the instance id unique per process (production keeps it stable).
        $conf->set('group.instance.id', sprintf('%s-%d', gethostname() ?: 'demo', getmypid()));
        $conf->set('statistics.interval.ms', (string) $statsInterval);

        $this->wireRebalanceCallback($conf, $output);
        $this->wireStatsCallback($conf, $output);
        $conf->setErrorCb(function ($consumer, int $err, string $reason) use ($output): void {
            $output->writeln(sprintf('<error>librdkafka error %s: %s</error>', rd_kafka_err2str($err), $reason));
        });

        $output->writeln(sprintf('<comment>raw rdkafka consumer · topic=%s group=%s stats-interval=%dms</comment>', $topic, $group, $statsInterval));
        $output->writeln('<comment>(this command bypasses enqueue on purpose — setStatsCb/setRebalanceCb have no enqueue surface)</comment>');

        $consumer = new \RdKafka\KafkaConsumer($conf);
        $consumer->subscribe([$topic]);

        $processed = 0;
        $start = time();
        $deadline = $runtime > 0 ? $start + $runtime : 0;

        while ($running) {
            if ($max > 0 && $processed >= $max) {
                break;
            }
            if ($deadline > 0 && time() >= $deadline) {
                break;
            }

            // consume() also drives the stats/rebalance/error callbacks.
            $message = $consumer->consume($timeoutMs);
            if (! $running) {
                break;
            }

            switch ($message->err) {
                case RD_KAFKA_RESP_ERR_NO_ERROR:
                    if ($slowMs > 0) {
                        usleep($slowMs * 1000); // simulate slow processing
                    }
                    // Real processing would go here; we only count for the demo.
                    // Commit AFTER the work (sync) — at-least-once. php-rdkafka's
                    // high-level consumer has no storeOffsets(); explicit commit is
                    // the equivalent, and is what enqueue's acknowledge() does.
                    $consumer->commit($message);
                    ++$processed;
                    $output->writeln(sprintf('  <info>✓</info> partition=%d offset=%d key=%s', $message->partition, $message->offset, $message->key ?? '<null>'));
                    break;

                case RD_KAFKA_RESP_ERR__TIMED_OUT:
                case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                    break;

                default:
                    $output->writeln(sprintf('  <error>consume error: %s</error>', $message->errstr()));
                    break;
            }
        }

        $this->shutdown($consumer, $output, $processed, time() - $start);

        return Command::SUCCESS;
    }

    /**
     * Cooperative-sticky requires incremental (un)assign instead of the eager
     * assign()/assign(null). Only the partitions that actually move are touched,
     * so a rebalance no longer stops every consumer in the group.
     */
    private function wireRebalanceCallback(\RdKafka\Conf $conf, OutputInterface $output): void
    {
        $conf->setRebalanceCb(function (\RdKafka\KafkaConsumer $consumer, int $err, ?array $partitions = null) use ($output): void {
            $names = array_map(
                static fn (\RdKafka\TopicPartition $p): string => sprintf('%s[%d]', $p->getTopic(), $p->getPartition()),
                $partitions ?? [],
            );

            switch ($err) {
                case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
                    $output->writeln(sprintf('  <comment>⇄ assign %s</comment>', implode(', ', $names) ?: '(none)'));
                    $consumer->incrementalAssign($partitions ?? []);
                    break;

                case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
                    $output->writeln(sprintf('  <comment>⇄ revoke %s</comment>', implode(', ', $names) ?: '(none)'));
                    $consumer->incrementalUnassign($partitions ?? []);
                    break;

                default:
                    $output->writeln(sprintf('  <error>rebalance error: %s</error>', rd_kafka_err2str($err)));
                    $consumer->assign(null);
            }
        });
    }

    /**
     * The headline of Block 8: librdkafka emits a JSON stats blob every
     * statistics.interval.ms, and this callback is the ONLY way a PHP consumer
     * sees client-internal numbers (PHP has no JMX). We surface the three that
     * matter most: per-partition consumer lag, broker RTT, and fetch-queue depth.
     */
    private function wireStatsCallback(\RdKafka\Conf $conf, OutputInterface $output): void
    {
        $conf->setStatsCb(function ($consumer, string $json) use ($output): void {
            $stats = json_decode($json, true);
            if (! is_array($stats)) {
                return;
            }

            $output->writeln('  <info>── stats ──────────────────────────────</info>');

            foreach ($stats['brokers'] ?? [] as $broker) {
                $rttMs = isset($broker['rtt']['avg']) ? $broker['rtt']['avg'] / 1000 : 0.0;
                if (($broker['nodeid'] ?? -1) < 0 && 0.0 === $rttMs) {
                    continue; // bootstrap pseudo-broker with no traffic yet
                }
                $output->writeln(sprintf(
                    '  broker %s state=%s rtt=%.2fms',
                    $broker['name'] ?? '?',
                    $broker['state'] ?? '?',
                    $rttMs,
                ));
            }

            foreach ($stats['topics'] ?? [] as $topicName => $topic) {
                foreach ($topic['partitions'] ?? [] as $partitionId => $partition) {
                    $lag = $partition['consumer_lag'] ?? -1;
                    if ((int) $partitionId < 0 || $lag < 0) {
                        continue; // skip the internal aggregate partition
                    }
                    $output->writeln(sprintf(
                        '  %s[%s] lag=%d fetchq=%d stored=%d committed=%d',
                        $topicName,
                        $partitionId,
                        $lag,
                        $partition['fetchq_cnt'] ?? 0,
                        $partition['stored_offset'] ?? -1,
                        $partition['committed_offset'] ?? -1,
                    ));
                }
            }
        });
    }

    private function shutdown(\RdKafka\KafkaConsumer $consumer, OutputInterface $output, int $processed, int $elapsed): void
    {
        $output->writeln(sprintf('<comment>shutting down — processed %d message(s) in %ds</comment>', $processed, $elapsed));

        // Each message was already committed synchronously after processing, so
        // there is nothing extra to flush here. close() commits final offsets and
        // sends LeaveGroup, so the coordinator rebalances the remaining members
        // immediately instead of waiting out session.timeout.ms.
        $consumer->close();
        $output->writeln('<info>consumer closed cleanly (LeaveGroup sent)</info>');
    }

    /**
     * A named group keeps committed offsets across runs; an ephemeral id reads
     * from earliest every run so the backlog — and the lag draining to zero — is
     * visible in the stats output.
     */
    private function resolveGroup(?string $group, string $topic): string
    {
        if (null !== $group) {
            return $group;
        }

        return sprintf('config-stats-%s-%d-%d', $topic, getmypid(), time());
    }
}
