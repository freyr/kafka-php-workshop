<?php

declare(strict_types=1);

namespace Workshop\Kafka\Client;

use RdKafka\Exception;
use RdKafka\KafkaConsumer;
use RdKafka\Message;
use Workshop\Kafka\Runtime\CommitPolicy;
use Workshop\Kafka\Runtime\RunLimits;

/**
 * A typed wrapper over \RdKafka\KafkaConsumer — the consume-side mirror of
 * MessageProducer. Where the producer's single public verb is produce(), the
 * consumer's is run(): it owns the consume loop so a command supplies only a
 * per-message handler.
 *
 * run() hides the boilerplate every consumer block would otherwise re-derive: the
 * NO_ERROR/TIMED_OUT/PARTITION_EOF/error switch, the at-least-once commit AFTER a
 * successful handler, the max-message / runtime / idle stop conditions, graceful
 * SIGTERM/SIGINT shutdown, and close() (immediate LeaveGroup) in a finally so the
 * group rebalances now instead of waiting out session.timeout.ms.
 *
 * Pass a narrator to surface assign/commit/EOF lines — the Block 1 verbose mode
 * that makes the loop's mechanics visible.
 */
final readonly class MessageConsumer
{
    public function __construct(
        private KafkaConsumer $consumer,
    ) {
    }

    /**
     * @param list<string>                  $topics
     * @param callable(Message): void       $handler
     * @param (\Closure(string): void)|null $narrate
     *
     * @return int the number of messages processed
     *
     * @throws Exception
     */
    public function run(
        array $topics,
        callable $handler,
        RunLimits $limits,
        CommitPolicy $policy,
        ?\Closure $narrate = null,
        int $pauseMs = 0,
    ): int {
        $this->consumer->subscribe($topics);

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
                $message = $this->consumer->consume($limits->pollTimeoutMs);
                if (! $running) {
                    break;
                }

                switch ($message->err) {
                    case RD_KAFKA_RESP_ERR_NO_ERROR:
                        $handler($message);
                        if (CommitPolicy::AfterEachMessage === $policy) {
                            $this->consumer->commit($message);
                            $this->say($narrate, sprintf('✓ committed partition=%d offset=%d', $message->partition, $message->offset));
                        }
                        ++$processed;

                        // Throttle between messages. usleep is interrupted by an
                        // async SIGINT/SIGTERM, so the loop re-checks $running right
                        // after instead of waiting the pause out.
                        if ($pauseMs > 0) {
                            usleep($pauseMs * 1000);
                        }

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
            $this->consumer->close();
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
