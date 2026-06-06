<?php

declare(strict_types=1);

namespace Workshop\Kernel;

/**
 * The workshop's recommended production producer/consumer configuration, as a
 * single source of truth. `config:show` renders these tables; `config:stats`
 * feeds the consumer values straight into a raw RdKafka\Conf.
 *
 * Defaults quoted here are librdkafka's own (see CONFIGURATION.md) so the
 * "value vs default" column shows exactly what we are changing and why. With one
 * exception these values are NOT wired into KafkaContextFactory's globals —
 * flipping compression or idempotence for every command would change the other
 * blocks' demos, so they stay opt-in via the injection point
 * (`forProducer($overrides)` / `forConsumer($group, $overrides)`). The exception
 * is `partition.assignment.strategy=cooperative-sticky`, which IS the factory
 * default for every consumer (it is safe — each command is its own group and
 * librdkafka handles the incremental rebalance internally).
 */
final readonly class KafkaTuning
{
    /**
     * @return list<KafkaSetting>
     */
    public function producer(): array
    {
        return [
            new KafkaSetting('reliability', 'enable.idempotence', 'true', 'false', 'Exactly-once delivery to the broker; auto-sets acks=all, max.in.flight=5, retries=MAX_INT. Always on in production.'),
            new KafkaSetting('reliability', 'acks', 'all', 'all', 'All in-sync replicas ack before success. Forced to all by idempotence anyway; stated for clarity.'),
            new KafkaSetting('batching', 'linger.ms', '50', '5', 'Wait up to 50ms to fill a batch. The latency-vs-throughput knob; 50 trades a little latency for better batching + compression.'),
            new KafkaSetting('batching', 'batch.num.messages', '10000', '10000', 'Max messages per produce request. Default is fine until very high volume.'),
            new KafkaSetting('batching', 'batch.size', '1000000', '1000000', 'Max bytes per batch (~1MB). Must stay under the broker message.max.bytes after compression.'),
            new KafkaSetting('compression', 'compression.type', 'lz4', 'none', 'Best speed-to-ratio. AVRO batches share structure and still compress well; switch to zstd only when storage/bandwidth is the constraint.'),
            new KafkaSetting('queue', 'queue.buffering.max.messages', '100000', '100000', 'Cap on the in-memory producer queue. Raise for bursty/batch producers.'),
            new KafkaSetting('queue', 'queue.buffering.max.kbytes', '262144', '1048576', 'Cap the producer queue at 256MB (not the 1GB default) so it can never exhaust a 256MB PHP worker.'),
            new KafkaSetting('timeouts', 'delivery.timeout.ms', '300000', '300000', 'Total umbrella for buffer+send+retries (5 min). Must be >= linger.ms + request.timeout.ms.'),
            new KafkaSetting('timeouts', 'request.timeout.ms', '30000', '30000', 'Per produce-request timeout before a retry inside the delivery umbrella.'),
        ];
    }

    /**
     * @return list<KafkaSetting>
     */
    public function consumer(): array
    {
        return [
            new KafkaSetting('offset', 'auto.offset.reset', 'earliest', 'largest', 'With no committed offset, start from the beginning so event-driven consumers process the full backlog (not just new events).'),
            new KafkaSetting('offset', 'enable.auto.commit', 'false', 'true', 'The key one: commit explicitly with commit($msg) AFTER processing succeeds = at-least-once. Background auto-commit (the default) can commit a message before your handler finishes and lose it on a crash. (The Confluent "auto.commit + offset-store" pattern is not exposed by php-rdkafka\'s high-level KafkaConsumer; explicit commit is the equivalent here, and is exactly what enqueue does.)'),
            new KafkaSetting('group-health', 'session.timeout.ms', '45000', '45000', '"Is the process alive?" — broker evicts the member after this with no heartbeat (heartbeats run on a background thread).'),
            new KafkaSetting('group-health', 'heartbeat.interval.ms', '3000', '3000', 'Send a heartbeat every 3s. Rule of thumb: <= session.timeout.ms / 3 (gives 2 chances to miss one).'),
            new KafkaSetting('group-health', 'max.poll.interval.ms', '300000', '300000', '"Is it making progress?" — main-thread budget between consume() calls. Raise it if a handler can take longer than 5 min, or the member gets ejected mid-work.'),
            new KafkaSetting('assignment', 'partition.assignment.strategy', 'cooperative-sticky', 'range,roundrobin', 'Incremental rebalancing: only moving partitions are revoked, the rest keep processing. Kills the stop-the-world rebalance storm during rolling deploys.'),
            new KafkaSetting('assignment', 'group.instance.id', 'gethostname()', '(none)', 'Static membership: a restart with the same id rejoins without a full rebalance.'),
            new KafkaSetting('fetch', 'fetch.min.bytes', '1', '1', 'Respond as soon as any data is available — lowest latency. Raise to batch fetches on low-volume topics.'),
            new KafkaSetting('fetch', 'max.partition.fetch.bytes', '1048576', '1048576', 'Max bytes per partition per fetch. Raise it if messages are large; must be >= broker max.message.bytes.'),
            new KafkaSetting('monitoring', 'statistics.interval.ms', '10000', '0', 'Emit the librdkafka stats JSON every 10s. PHP has no JMX — this callback is the only window into client-internal lag, RTT, and queue depth.'),
        ];
    }

    /**
     * Producer settings as plain key => value, shaped to drop into
     * KafkaContextFactory::forProducer($overrides). gethostname()-style dynamic
     * values are producer-irrelevant and omitted here.
     *
     * @return array<string, string>
     */
    public function producerOverrides(): array
    {
        return $this->toOverrides($this->producer());
    }

    /**
     * Consumer settings as plain key => value for a raw RdKafka\Conf or
     * KafkaContextFactory::forConsumer($group, $overrides). group.instance.id is
     * resolved to the real hostname here rather than the literal placeholder.
     *
     * @return array<string, string>
     */
    public function consumerOverrides(): array
    {
        $overrides = $this->toOverrides($this->consumer());
        $overrides['group.instance.id'] = gethostname() ?: 'php-consumer';

        return $overrides;
    }

    /**
     * @param list<KafkaSetting> $settings
     *
     * @return array<string, string>
     */
    private function toOverrides(array $settings): array
    {
        $out = [];
        foreach ($settings as $setting) {
            // Skip placeholder values that are not literal librdkafka input.
            if (str_ends_with($setting->value, ')')) {
                continue;
            }
            $out[$setting->key] = $setting->value;
        }

        return $out;
    }
}
