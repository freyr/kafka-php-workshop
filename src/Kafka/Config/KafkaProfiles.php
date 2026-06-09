<?php

declare(strict_types=1);

namespace Workshop\Kafka\Config;

/**
 * The catalog of named client profiles the workshop ships. A profile is nothing
 * more than a named set of KafkaSettings for one client role — "more than one
 * client with different config sets". Unlike the old model, a profile is NOT a
 * subset filtered out of one big tuning table: every profile owns its complete
 * set directly, composed here from small reusable config groups, and ConfBuilder
 * applies all of it wholesale. To override a key for a single run, pass it as a
 * runtime override to ConfBuilder::build() — last write wins.
 *
 * This is the multi-client seam: a command picks a profile by name and the
 * factory builds the matching client. Later blocks extend the workshop by adding
 * entries to all() (B5 producer.transactional / consumer.read-committed, B7
 * consumer.retry / producer.dlq, B8 consumer.observer-stats) — no factory changes.
 *
 * The KafkaSetting value objects keep their default + rationale, so every value a
 * profile applies stays defensible right where it is declared.
 */
final readonly class KafkaProfiles
{
    public function get(string $name): KafkaProfile
    {
        foreach ($this->all() as $profile) {
            if ($profile->name === $name) {
                return $profile;
            }
        }

        $known = implode(', ', array_map(static fn (KafkaProfile $p): string => $p->name, $this->all()));

        throw new \InvalidArgumentException(sprintf('Unknown Kafka profile "%s". Known profiles: %s', $name, $known));
    }

    /**
     * @return list<KafkaProfile>
     */
    public function all(): array
    {
        return [
            // Block 1: deliberately bare — librdkafka's own defaults, so the room
            // can see the cost of an untuned producer before we add reliability.
            new KafkaProfile('producer.simple', ClientRole::Producer, []),

            // Block 3: exactly-once-to-broker AVRO producer carrying the full
            // production tuning — reliability + compression + batching + queue +
            // timeouts, every value the workshop recommends actually applied.
            new KafkaProfile('producer.idempotent', ClientRole::Producer, [
                ...$this->reliability(),
                ...$this->compression(),
                ...$this->batching(),
                ...$this->queue(),
                ...$this->timeouts(),
            ]),

            // Throwaway inspector: reads the whole log from earliest and never
            // commits, so nothing it does is durable. No group.instance.id (a
            // throwaway group must not be fenced on re-run) and no assignment
            // override (the lone member needs no rebalancing strategy). The command
            // pairs this with a unique group id and a skip-only handler.
            new KafkaProfile('consumer.ephemeral', ClientRole::Consumer, [
                ...$this->offsetReset(),
                ...$this->manualCommit(),
            ]),

            // The classic committing consumer: librdkafka background auto-commit and
            // EAGER rebalancing (range,roundrobin) — every join/leave revokes the
            // whole assignment, the stop-the-world behavior. No static membership.
            // The deliberate contrast to consumer.modern.
            new KafkaProfile('consumer.default', ClientRole::Consumer, [
                ...$this->offsetReset(),
                ...$this->autoCommit(),
                ...$this->eagerAssignment(),
            ]),

            // The modern production consumer: explicit commit after each handler,
            // cooperative-sticky (incremental) rebalancing so only moving partitions
            // are revoked, plus static membership so a restart rejoins WITHOUT a
            // rebalance. Delivery guarantees (at-least-once / effectively-once) are
            // an orthogonal handler/DB concern, layered on with --idempotent.
            new KafkaProfile('consumer.modern', ClientRole::Consumer, [
                ...$this->offsetReset(),
                ...$this->manualCommit(),
                ...$this->cooperativeAssignment(),
                ...$this->staticMembership(),
            ]),
        ];
    }

    /**
     * Exactly-once delivery to the broker. enable.idempotence alone implies
     * acks=all; acks is stated for clarity.
     *
     * @return list<KafkaSetting>
     */
    private function reliability(): array
    {
        return [
            new KafkaSetting('reliability', 'enable.idempotence', 'true', 'false', 'Exactly-once delivery to the broker; auto-sets acks=all, max.in.flight=5, retries=MAX_INT. Always on in production.'),
            new KafkaSetting('reliability', 'acks', 'all', 'all', 'All in-sync replicas ack before success. Forced to all by idempotence anyway; stated for clarity.'),
        ];
    }

    /**
     * @return list<KafkaSetting>
     */
    private function compression(): array
    {
        return [
            new KafkaSetting('compression', 'compression.type', 'lz4', 'none', 'Best speed-to-ratio. AVRO batches share structure and still compress well; switch to zstd only when storage/bandwidth is the constraint.'),
        ];
    }

    /**
     * The latency-vs-throughput knob and its batch-size ceilings.
     *
     * @return list<KafkaSetting>
     */
    private function batching(): array
    {
        return [
            new KafkaSetting('batching', 'linger.ms', '50', '5', 'Wait up to 50ms to fill a batch. The latency-vs-throughput knob; 50 trades a little latency for better batching + compression.'),
            new KafkaSetting('batching', 'batch.num.messages', '10000', '10000', 'Max messages per produce request. Default is fine until very high volume.'),
            new KafkaSetting('batching', 'batch.size', '1000000', '1000000', 'Max bytes per batch (~1MB). Must stay under the broker message.max.bytes after compression.'),
        ];
    }

    /**
     * Bound the in-memory producer queue so it can never exhaust the PHP worker.
     *
     * @return list<KafkaSetting>
     */
    private function queue(): array
    {
        return [
            new KafkaSetting('queue', 'queue.buffering.max.messages', '100000', '100000', 'Cap on the in-memory producer queue. Raise for bursty/batch producers.'),
            new KafkaSetting('queue', 'queue.buffering.max.kbytes', '262144', '1048576', 'Cap the producer queue at 256MB (not the 1GB default) so it can never exhaust a 256MB PHP worker.'),
        ];
    }

    /**
     * The delivery umbrella and the per-request timeout that lives inside it.
     *
     * @return list<KafkaSetting>
     */
    private function timeouts(): array
    {
        return [
            new KafkaSetting('timeouts', 'delivery.timeout.ms', '300000', '300000', 'Total umbrella for buffer+send+retries (5 min). Must be >= linger.ms + request.timeout.ms.'),
            new KafkaSetting('timeouts', 'request.timeout.ms', '30000', '30000', 'Per produce-request timeout before a retry inside the delivery umbrella.'),
        ];
    }

    /**
     * With no committed offset, start from the earliest record so an event-driven
     * consumer processes the full backlog rather than only new events. Where a run
     * starts *relative to a committed offset* is a separate, per-run concern (the
     * --from seek), not a profile setting.
     *
     * @return list<KafkaSetting>
     */
    private function offsetReset(): array
    {
        return [
            new KafkaSetting('offset', 'auto.offset.reset', 'earliest', 'largest', 'With no committed offset, start from the beginning so event-driven consumers process the full backlog (not just new events).'),
        ];
    }

    /**
     * Commit explicitly from the run-loop AFTER the handler returns. The control the
     * application keeps over when the offset advances is what makes at-least-once
     * (and, with handler/DB dedup, effectively-once) possible.
     *
     * @return list<KafkaSetting>
     */
    private function manualCommit(): array
    {
        return [
            new KafkaSetting('offset', 'enable.auto.commit', 'false', 'true', 'The key one: commit explicitly with commit($msg) AFTER processing succeeds = at-least-once. Background auto-commit (the default) can commit a message before your handler finishes and lose it on a crash. (The Confluent "auto.commit + offset-store" pattern is not exposed by php-rdkafka\'s high-level KafkaConsumer; explicit commit is the equivalent here, and is exactly what enqueue does.)'),
        ];
    }

    /**
     * Let librdkafka commit offsets on a background timer (the default). Lowest
     * overhead, but a commit can land before the handler finishes — at-most-once
     * under failure. The interval is left at librdkafka's default.
     *
     * @return list<KafkaSetting>
     */
    private function autoCommit(): array
    {
        return [
            new KafkaSetting('offset', 'enable.auto.commit', 'true', 'true', 'Background auto-commit on a timer. Lowest overhead, but the offset can advance before the handler finishes, so a crash loses the in-flight message — at-most-once.'),
        ];
    }

    /**
     * Cooperative-sticky (incremental) rebalancing: only the partitions that move
     * are revoked, the rest keep processing.
     *
     * @return list<KafkaSetting>
     */
    private function cooperativeAssignment(): array
    {
        return [
            new KafkaSetting('assignment', 'partition.assignment.strategy', 'cooperative-sticky', 'range,roundrobin', 'Incremental rebalancing: only moving partitions are revoked, the rest keep processing. Kills the stop-the-world rebalance storm during rolling deploys.'),
        ];
    }

    /**
     * Eager rebalancing (range,roundrobin) — librdkafka's own default, stated
     * explicitly so the eager-vs-cooperative contrast is visible in the profile.
     * Every join/leave revokes the whole assignment before reassigning it: the
     * stop-the-world rebalance.
     *
     * @return list<KafkaSetting>
     */
    private function eagerAssignment(): array
    {
        return [
            new KafkaSetting('assignment', 'partition.assignment.strategy', 'range,roundrobin', 'range,roundrobin', 'Eager rebalancing: every join/leave revokes the entire assignment, then reassigns — the stop-the-world pause that cooperative-sticky avoids.'),
        ];
    }

    /**
     * @return list<KafkaSetting>
     */
    private function staticMembership(): array
    {
        return [
            new KafkaSetting('assignment', 'group.instance.id', 'gethostname()', '(none)', 'Static membership: a restart with the same id rejoins without a full rebalance.'),
        ];
    }
}
