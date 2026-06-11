<?php

declare(strict_types=1);

namespace Workshop\Kafka\Client;

use RdKafka\Exception;
use RdKafka\KafkaConsumer;
use RdKafka\Message;
use Workshop\Kafka\Runtime\CommitPolicy;
use Workshop\Kafka\Runtime\RebalanceCommitError;
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
    /**
     * How long a single consume() blocks waiting for a record. A fixed cadence, not
     * a stop condition: short enough that the loop reacts to Ctrl-C and re-checks its
     * stop conditions promptly, long enough that an empty poll does not busy-spin.
     */
    private const int POLL_TIMEOUT_MS = 1000;

    public function __construct(
        private KafkaConsumer $consumer,
    ) {
    }

    /**
     * @param list<string>                  $topics
     * @param callable(Message): void       $handler
     * @param (\Closure(string): void)|null $narrate
     * @param \Closure|null                 $onSignal also invoked on SIGINT/SIGTERM —
     *                                                lets a handler that loops or
     *                                                sleeps (the --errors retry path)
     *                                                observe the stop request the
     *                                                loop's own flag tracks
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
        ?\Closure $onSignal = null,
    ): int {
        $this->consumer->subscribe($topics);

        $running = true;
        pcntl_async_signals(true);
        $stop = static function () use (&$running, $onSignal): void {
            $running = false;
            if (null !== $onSignal) {
                $onSignal();
            }
        };
        pcntl_signal(SIGTERM, $stop);
        pcntl_signal(SIGINT, $stop);

        $processed = 0;
        // Monotonic millisecond clock for the TTL deadline — immune to wall-clock
        // adjustments, and ms-granular so a sub-second --ttl is honored.
        $startedAtMs = $this->nowMs();
        // The last message whose handler returned successfully — committed
        // synchronously on close under AsyncAfterEachMessage so the final offset is
        // durable. Stays null until the first message is handled.
        $lastHandled = null;

        try {
            while ($running) {
                if ($limits->reachedMax($processed) || $limits->deadlinePassed($startedAtMs, $this->nowMs())) {
                    break;
                }

                // consume() also drives the rebalance/error callbacks on the conf.
                $message = $this->consumer->consume(self::POLL_TIMEOUT_MS);
                if (! $running) {
                    break;
                }

                switch ($message->err) {
                    case RD_KAFKA_RESP_ERR_NO_ERROR:
                        $handler($message);
                        if (CommitPolicy::AfterEachMessage === $policy) {
                            $this->commitSync($message, $narrate);
                        } elseif (CommitPolicy::AsyncAfterEachMessage === $policy) {
                            // Fire-and-forget: no broker round-trip blocks the loop.
                            $this->consumer->commitAsync($message);
                            $lastHandled = $message;
                            $this->say($narrate, sprintf('→ commit queued (async) partition=%d offset=%d', $message->partition, $message->offset));
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
                        // An empty poll only means "drained" once the group has
                        // actually assigned partitions: before the first rebalance
                        // completes EVERY poll times out, and stopping there would
                        // end a --drain run that never read anything.
                        if ($limits->stopOnIdle && [] !== $this->consumer->getAssignment()) {
                            $running = false;
                        }

                        break;

                    default:
                        $this->say($narrate, 'consume error: ' . $message->errstr());
                }
            }
        } finally {
            // Under async commits the per-message commitAsync() may not have landed
            // yet. One synchronous commit of the LAST HANDLED message makes the final
            // offset durable before we leave the group. Committing that specific
            // message — not the no-arg consumed position — is what keeps this
            // at-least-once: a message whose handler threw was never recorded as
            // handled, so it stays uncommitted and is reprocessed next run.
            if (CommitPolicy::AsyncAfterEachMessage === $policy && null !== $lastHandled) {
                try {
                    $this->consumer->commit($lastHandled);
                    $this->say($narrate, sprintf('✓ final sync commit partition=%d offset=%d', $lastHandled->partition, $lastHandled->offset));
                } catch (Exception $e) {
                    $this->say($narrate, 'final commit failed: ' . $e->getMessage());
                }
            }

            // Final LeaveGroup, even if the handler threw.
            $this->consumer->close();
        }

        return $processed;
    }

    /**
     * Synchronous commit of one message's offset, tolerating the failure a rebalance
     * makes expected. Between handling a record and committing it, a rebalance can
     * revoke that partition or advance the group generation; the broker then rejects
     * the commit (ILLEGAL_GENERATION / REBALANCE_IN_PROGRESS / UNKNOWN_MEMBER_ID).
     * That is not fatal: the handler already ran, but the offset is no longer ours to
     * commit, so the record is redelivered to the partition's new owner — at-least-once
     * still holds. Any other commit error is a genuine fault and propagates.
     */
    private function commitSync(Message $message, ?\Closure $narrate): void
    {
        try {
            $this->consumer->commit($message);
            $this->say($narrate, sprintf('✓ committed partition=%d offset=%d', $message->partition, $message->offset));
        } catch (Exception $e) {
            if (! RebalanceCommitError::matches($e->getCode())) {
                throw $e;
            }
            $this->say($narrate, sprintf('⚠ commit skipped — partition=%d reassigned mid-rebalance; offset %d will be redelivered', $message->partition, $message->offset));
        }
    }

    private function say(?\Closure $narrate, string $line): void
    {
        if (null !== $narrate) {
            $narrate($line);
        }
    }

    private function nowMs(): int
    {
        return intdiv(hrtime(true), 1_000_000);
    }
}
