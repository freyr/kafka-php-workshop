<?php

declare(strict_types=1);

namespace Workshop\Kafka\Runtime;

/**
 * Owns the consume loop so a command supplies only a per-message handler. It hides
 * the boilerplate every consumer block would otherwise re-derive: the
 * NO_ERROR/TIMED_OUT/PARTITION_EOF/error switch, the at-least-once commit AFTER a
 * successful handler, the max-message / runtime / idle stop conditions, graceful
 * SIGTERM/SIGINT shutdown, and close() (immediate LeaveGroup) in a finally so the
 * group rebalances now instead of waiting out session.timeout.ms.
 *
 * Pass a narrator to surface assign/commit/EOF lines — the Block 1 verbose mode
 * that makes the loop's mechanics visible.
 */
final readonly class ConsumerRunner
{
    /**
     * @param list<string>                     $topics
     * @param callable(\RdKafka\Message): void $handler
     * @param (\Closure(string): void)|null    $narrate
     *
     * @return int the number of messages processed
     */
    public function run(
        \RdKafka\KafkaConsumer $consumer,
        array $topics,
        callable $handler,
        RunLimits $limits,
        CommitPolicy $policy,
        ?\Closure $narrate = null,
    ): int {
        $consumer->subscribe($topics);

        $running = true;
        pcntl_async_signals(true);
        $stop = static function () use (&$running): void {
            $running = false;
        };
        pcntl_signal(SIGTERM, $stop);
        pcntl_signal(SIGINT, $stop);

        $processed = 0;
        $startedAt = time();

        try {
            while ($running) {
                if ($limits->reachedMax($processed) || $limits->deadlinePassed($startedAt, time())) {
                    break;
                }

                // consume() also drives the rebalance/error callbacks on the conf.
                $message = $consumer->consume($limits->pollTimeoutMs);
                if (! $running) {
                    break;
                }

                switch ($message->err) {
                    case RD_KAFKA_RESP_ERR_NO_ERROR:
                        $handler($message);
                        if (CommitPolicy::AfterEachMessage === $policy) {
                            $consumer->commit($message);
                            $this->say($narrate, sprintf('✓ committed partition=%d offset=%d', $message->partition, $message->offset));
                        }
                        ++$processed;

                        break;

                    case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                        $this->say($narrate, 'reached end of partition');
                        if ($limits->stopOnIdle) {
                            $running = false;
                        }

                        break;

                    case RD_KAFKA_RESP_ERR__TIMED_OUT:
                        if ($limits->stopOnIdle) {
                            $running = false;
                        }

                        break;

                    default:
                        $this->say($narrate, 'consume error: ' . $message->errstr());
                }
            }
        } finally {
            // Final commit + LeaveGroup, even if the handler threw.
            $consumer->close();
        }

        return $processed;
    }

    private function say(?\Closure $narrate, string $line): void
    {
        if (null !== $narrate) {
            $narrate($line);
        }
    }
}
