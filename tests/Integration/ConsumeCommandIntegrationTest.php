<?php

declare(strict_types=1);

namespace Workshop\Tests\Integration;

use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Output\OutputInterface;

/**
 * kafka:consume against the live stack, one test per profile/flag behavior. Every
 * consuming run is double-bounded — --max pinned to the probed backlog (or an
 * explicit cap) plus a --ttl ceiling — so no test can hang on a wedged consumer.
 */
final class ConsumeCommandIntegrationTest extends IntegrationTestCase
{
    private const string ORDERS_TOPIC = 'enet.ecommerce.orders';

    /**
     * Single-partition topic: offsets are totally ordered, which the commit/resume
     * test relies on. order.audited has no DB handler (decode-and-flow by design).
     */
    private const string AUDIT_TOPIC = 'enet.ecommerce.audit';

    private const string DEMO_TOPIC = 'enet.demo.orders';

    public function testBinaryEntryPointInspectsAndExits(): void
    {
        $this->produce(1, 'order.audited');

        $result = self::console(sprintf('kafka:consume %s --max 1 --ttl 30000', self::AUDIT_TOPIC));

        self::assertSame(0, $result['exit'], $result['output']);
        self::assertStringContainsString('done — inspected 1 message(s)', $result['output']);
    }

    public function testEphemeralInspectsEverythingAndProjectsNothing(): void
    {
        $this->produce(5, 'order.created');
        $backlog = $this->probe()->totalEnd(self::ORDERS_TOPIC);

        $tester = $this->consumeBacklog(self::ORDERS_TOPIC);

        self::assertStringContainsString(sprintf('done — inspected %d message(s)', $backlog), $tester->getDisplay());
        self::assertMatchesRegularExpression('/• order\.created id=\S+ partition=\d+ offset=\d+/u', $tester->getDisplay());
        self::assertSame(0, $this->countRows('orders'), 'ephemeral must never project');
        self::assertSame(0, $this->countRows('processed_events'), 'ephemeral must never record dedup state');
    }

    public function testDefaultProfileProjectsOrders(): void
    {
        $keys = self::producedKeys($this->produce(3, 'order.created'));

        $tester = $this->consumeBacklog(self::ORDERS_TOPIC, [
            '--profile' => 'default',
            '--group' => $this->uniqueGroup(),
        ]);

        self::assertStringContainsString('✓ order.created order=', $tester->getDisplay());
        self::assertMatchesRegularExpression('/done — handled \d+, skipped \d+/u', $tester->getDisplay());
        foreach (array_unique($keys) as $key) {
            $status = $this->db()->fetchOne('SELECT status FROM orders WHERE order_id = ?', [$key]);
            self::assertSame('open', $status, sprintf('order %s must be projected as open', $key));
        }
    }

    public function testOrderLifecycleColumnEffects(): void
    {
        $created = self::producedKeys($this->produce(1, 'order.created', [
            '--pool' => '1',
        ]))[0];
        $updated = self::producedKeys($this->produce(1, 'order.updated', [
            '--pool' => '1',
        ]))[0];
        $cancelled = self::producedKeys($this->produce(1, 'order.cancelled', [
            '--pool' => '1',
        ]))[0];

        $this->consumeBacklog(self::ORDERS_TOPIC, [
            '--profile' => 'default',
            '--group' => $this->uniqueGroup(),
        ]);

        $createdRow = $this->row($created);
        self::assertSame('open', $createdRow['status']);
        self::assertNull($createdRow['cancelled_reason']);

        $updatedRow = $this->row($updated);
        self::assertNotNull($updatedRow['status']);
        self::assertNotNull($updatedRow['previous_status'], 'order.updated must record the previous status');

        $cancelledRow = $this->row($cancelled);
        self::assertSame('cancelled', $cancelledRow['status']);
        self::assertNotNull($cancelledRow['cancelled_reason'], 'order.cancelled must record its reason');
    }

    public function testModernProfileCommitsAndResumes(): void
    {
        $this->produce(6, 'order.audited');
        $group = $this->uniqueGroup();

        $first = $this->runCommand('kafka:consume', [
            'topic' => self::AUDIT_TOPIC,
            '--profile' => 'modern',
            '--group' => $group,
            '--from' => 'beginning',
            '--max' => '3',
            '--ttl' => '60000',
            '--print' => true,
        ]);
        $second = $this->runCommand('kafka:consume', [
            'topic' => self::AUDIT_TOPIC,
            '--profile' => 'modern',
            '--group' => $group,
            '--from' => 'committed',
            '--max' => '3',
            '--ttl' => '60000',
            '--print' => true,
        ]);

        self::assertSame(Command::SUCCESS, $first->getStatusCode(), $first->getDisplay());
        self::assertSame(Command::SUCCESS, $second->getStatusCode(), $second->getDisplay());

        $firstOffsets = $this->printedOffsets($first->getDisplay());
        $secondOffsets = $this->printedOffsets($second->getDisplay());
        self::assertCount(3, $firstOffsets);
        self::assertCount(3, $secondOffsets);
        self::assertLessThan(
            min($secondOffsets),
            max($firstOffsets),
            'a committed resume must continue past everything the first run handled',
        );
    }

    public function testIdempotentReplayIsEffectivelyOnce(): void
    {
        $this->produce(4, 'order.created');

        $this->consumeBacklog(self::ORDERS_TOPIC, [
            '--profile' => 'modern',
            '--idempotent' => true,
            '--group' => $this->uniqueGroup(),
        ]);

        $processed = $this->countRows('processed_events');
        self::assertGreaterThanOrEqual(4, $processed);
        $orders = $this->db()->fetchAllAssociative('SELECT * FROM orders ORDER BY order_id');
        self::assertNotEmpty($orders);

        // Replay the whole log under a fresh group: every event is already in the
        // dedup ledger, so handlers must be skipped and nothing may change.
        $this->consumeBacklog(self::ORDERS_TOPIC, [
            '--profile' => 'modern',
            '--idempotent' => true,
            '--group' => $this->uniqueGroup(),
        ]);

        self::assertSame($processed, $this->countRows('processed_events'), 'a replay must not grow the dedup ledger');
        self::assertSame($orders, $this->db()->fetchAllAssociative('SELECT * FROM orders ORDER BY order_id'), 'a replay must not touch the projection');
    }

    public function testPrintDumpsRawRecordsWithoutProjecting(): void
    {
        $this->produce(2, 'order.created');

        $tester = $this->consumeBacklog(self::ORDERS_TOPIC, [
            '--profile' => 'default',
            '--group' => $this->uniqueGroup(),
            '--print' => true,
        ]);

        self::assertStringContainsString('raw decoded record (reader=writer)', $tester->getDisplay());
        // The dump shows the wire payload (AVRO field names), not the DTO shape.
        self::assertStringContainsString('"order_id"', $tester->getDisplay());
        self::assertSame(0, $this->countRows('orders'), '--print must bypass the DB handler');
        self::assertSame(0, $this->countRows('processed_events'), '--print must bypass the middleware');
    }

    public function testReaderLatestDecodesTheDemoTopic(): void
    {
        $this->produce(2, 'demo.order.evolved');

        $tester = $this->consumeBacklog(self::DEMO_TOPIC, [
            '--profile' => 'default',
            '--group' => $this->uniqueGroup(),
            '--reader' => 'latest',
        ]);

        self::assertStringContainsString('reader=latest', $tester->getDisplay());
        self::assertStringContainsString('✓ demo.order.evolved order=', $tester->getDisplay());
        self::assertMatchesRegularExpression('/orderId\s+= /u', $tester->getDisplay(), 'FieldPrintHandler must print the DTO fields');
    }

    public function testTtlStopsAnIdleConsumer(): void
    {
        $startedAt = microtime(true);

        $tester = $this->runCommand('kafka:consume', [
            'topic' => self::ORDERS_TOPIC,
            '--profile' => 'default',
            '--group' => $this->uniqueGroup(),
            '--from' => 'end',
            '--ttl' => '3000',
        ]);
        $elapsed = microtime(true) - $startedAt;

        self::assertSame(Command::SUCCESS, $tester->getStatusCode(), $tester->getDisplay());
        self::assertStringContainsString('done — handled 0, skipped 0', $tester->getDisplay());
        self::assertGreaterThanOrEqual(2.9, $elapsed, 'the consumer must live out its ttl');
        self::assertLessThan(20.0, $elapsed, 'the consumer must stop soon after its ttl');
    }

    public function testVerboseNarratesAssignments(): void
    {
        $this->produce(1, 'order.audited');

        $tester = $this->runCommand('kafka:consume', [
            'topic' => self::AUDIT_TOPIC,
            '--max' => '1',
            '--ttl' => '30000',
        ], [
            'verbosity' => OutputInterface::VERBOSITY_VERBOSE,
        ]);

        self::assertSame(Command::SUCCESS, $tester->getStatusCode(), $tester->getDisplay());
        self::assertStringContainsString('⇄ assign', $tester->getDisplay());
    }

    public function testInvalidOptionsAreRejected(): void
    {
        $cases = [
            'unknown offset reset' => [[
                '--from' => 'middle',
            ], 'Unknown offset reset'],
            'unknown profile' => [[
                '--profile' => 'turbo',
            ], 'Unknown consumer profile'],
            'unknown reader' => [[
                '--reader' => 'psychic',
            ], '--reader must be: writer | latest'],
        ];
        foreach ($cases as $label => [$input, $error]) {
            $tester = $this->runCommand('kafka:consume', $input + [
                'topic' => self::ORDERS_TOPIC,
            ]);

            self::assertSame(Command::INVALID, $tester->getStatusCode(), $label);
            self::assertStringContainsString($error, $tester->getDisplay(), $label);
        }
    }

    public function testCustomGroupAppearsInBanner(): void
    {
        $this->produce(1, 'order.audited');
        $group = $this->uniqueGroup();

        $tester = $this->runCommand('kafka:consume', [
            'topic' => self::AUDIT_TOPIC,
            '--profile' => 'modern',
            '--group' => $group,
            '--from' => 'beginning',
            '--max' => '1',
            '--ttl' => '60000',
        ]);

        self::assertSame(Command::SUCCESS, $tester->getStatusCode(), $tester->getDisplay());
        self::assertStringContainsString(sprintf('group=%s', $group), $tester->getDisplay());
    }

    public function testIntervalThrottleIsAccepted(): void
    {
        $this->produce(3, 'order.audited');

        $tester = $this->runCommand('kafka:consume', [
            'topic' => self::AUDIT_TOPIC,
            '--max' => '3',
            '--interval' => '200',
            '--ttl' => '60000',
        ]);

        self::assertSame(Command::SUCCESS, $tester->getStatusCode(), $tester->getDisplay());
        self::assertStringContainsString('done — inspected 3 message(s)', $tester->getDisplay());
    }

    private function countRows(string $table): int
    {
        return (int) $this->db()->fetchOne(sprintf('SELECT COUNT(*) FROM %s', $table));
    }

    /**
     * @return array<string, mixed>
     */
    private function row(string $orderId): array
    {
        $row = $this->db()->fetchAssociative('SELECT * FROM orders WHERE order_id = ?', [$orderId]);
        self::assertNotFalse($row, sprintf('order %s must exist in the projection', $orderId));

        return $row;
    }

    /**
     * The offsets a --print run dumped, one per record.
     *
     * @return list<int>
     */
    private function printedOffsets(string $display): array
    {
        preg_match_all('/• \S+ offset=(\d+) — raw decoded record/u', $display, $matches);

        return array_values(array_map(intval(...), $matches[1]));
    }
}
