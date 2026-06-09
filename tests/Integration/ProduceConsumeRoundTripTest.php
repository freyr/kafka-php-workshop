<?php

declare(strict_types=1);

namespace Workshop\Tests\Integration;

/**
 * Whole-pipeline runs: produce through the real command, consume through the real
 * command, assert on what actually landed in the projection — plus the
 * skip-not-crash contract for records the consumer cannot decode.
 */
final class ProduceConsumeRoundTripTest extends IntegrationTestCase
{
    private const string ORDERS_TOPIC = 'enet.ecommerce.orders';

    /**
     * order.audited has no DB handler, but under --idempotent the dedup middleware
     * still records each event — so the audit topic exercises the
     * effectively-once ledger on a second stream without touching `orders`.
     */
    private const string AUDIT_TOPIC = 'enet.ecommerce.audit';

    public function testMixedPipelineIsEffectivelyOnceAcrossTopics(): void
    {
        $producer = $this->produce(20, null, [
            '--pool' => '4',
        ]);

        preg_match_all('/^produced (\S+) → (\S+) key=(\S+)$/mu', $producer->getDisplay(), $sends, PREG_SET_ORDER);
        self::assertCount(20, $sends, 'every send must be reported');

        $orderKeys = [];
        foreach ($sends as [, $name, $topic, $key]) {
            if (self::ORDERS_TOPIC === $topic) {
                $orderKeys[$key] = true;
            }
        }

        $this->consumeBacklog(self::ORDERS_TOPIC, [
            '--profile' => 'modern',
            '--idempotent' => true,
            '--group' => $this->uniqueGroup(),
        ]);
        $this->consumeBacklog(self::AUDIT_TOPIC, [
            '--profile' => 'modern',
            '--idempotent' => true,
            '--group' => $this->uniqueGroup(),
        ]);

        foreach (array_keys($orderKeys) as $key) {
            self::assertNotFalse(
                $this->db()->fetchOne('SELECT order_id FROM orders WHERE order_id = ?', [$key]),
                sprintf('order %s produced to the orders topic must be projected', $key),
            );
        }

        $ledger = (int) $this->db()->fetchOne('SELECT COUNT(*) FROM processed_events');
        self::assertGreaterThan(0, $ledger);
        $orders = $this->db()->fetchAllAssociative('SELECT * FROM orders ORDER BY order_id');

        // Replay both topics under fresh groups: the ledger dedups every event, so
        // neither table may change.
        $this->consumeBacklog(self::ORDERS_TOPIC, [
            '--profile' => 'modern',
            '--idempotent' => true,
            '--group' => $this->uniqueGroup(),
        ]);
        $this->consumeBacklog(self::AUDIT_TOPIC, [
            '--profile' => 'modern',
            '--idempotent' => true,
            '--group' => $this->uniqueGroup(),
        ]);

        self::assertSame($ledger, (int) $this->db()->fetchOne('SELECT COUNT(*) FROM processed_events'));
        self::assertSame($orders, $this->db()->fetchAllAssociative('SELECT * FROM orders ORDER BY order_id'));
    }

    public function testPoisonRecordIsSkippedNotFatal(): void
    {
        // A bare record with no AVRO framing and no message-name header — exactly
        // what the interpreter must skip (never crash on).
        $conf = new \RdKafka\Conf();
        $conf->set('metadata.broker.list', self::brokers());
        $producer = new \RdKafka\Producer($conf);
        $producer->newTopic(self::ORDERS_TOPIC)->produce(RD_KAFKA_PARTITION_UA, 0, 'definitely-not-avro');
        self::assertSame(RD_KAFKA_RESP_ERR_NO_ERROR, $producer->flush(10000), 'the poison record must reach the broker');

        $keys = self::producedKeys($this->produce(2, 'order.created'));

        $tester = $this->consumeBacklog(self::ORDERS_TOPIC, [
            '--profile' => 'default',
            '--group' => $this->uniqueGroup(),
        ]);

        self::assertMatchesRegularExpression('/done — handled \d+, skipped [1-9]\d*/u', $tester->getDisplay(), 'the poison record must be counted as skipped');
        foreach (array_unique($keys) as $key) {
            self::assertSame(
                'open',
                $this->db()->fetchOne('SELECT status FROM orders WHERE order_id = ?', [$key]),
                sprintf('valid order %s must still be projected despite the poison record', $key),
            );
        }
    }
}
