<?php

declare(strict_types=1);

namespace Workshop\Tests\Integration;

use PHPUnit\Framework\Attributes\DataProvider;
use Symfony\Component\Console\Command\Command;

/**
 * kafka:produce:sample against the live stack. Topic effects are asserted as
 * watermark-offset deltas (never absolute counts), so the shared topics may carry
 * messages from earlier tests without breaking anything.
 */
final class ProduceCommandIntegrationTest extends IntegrationTestCase
{
    private const string ORDERS_TOPIC = 'enet.ecommerce.orders';

    /**
     * Every topic the producer routes to (config/producers.yaml) — the universe a
     * routing assertion has to prove untouched.
     */
    private const array ROUTED_TOPICS = [
        'enet.ecommerce.orders',
        'enet.ecommerce.payments',
        'enet.ecommerce.inventory',
        'enet.ecommerce.audit',
        'enet.demo.orders',
    ];

    public function testBinaryEntryPointProducesOneMessage(): void
    {
        $result = self::console('kafka:produce:sample -c 1 --interval 0');

        self::assertSame(0, $result['exit'], $result['output']);
        self::assertStringContainsString('done — produced 1 message(s)', $result['output']);
    }

    public function testProducesExactlyTheRequestedCount(): void
    {
        $before = $this->routedTotal();

        $tester = $this->produce(5);

        self::assertSame(5, preg_match_all('/^produced /m', $tester->getDisplay()));
        self::assertStringContainsString('done — produced 5 message(s)', $tester->getDisplay());
        self::assertSame(5, $this->routedTotal() - $before);
    }

    #[DataProvider('routes')]
    public function testPinnedMessageRoutesOnlyToItsTopic(string $messageName, string $topic): void
    {
        $before = [];
        foreach (self::ROUTED_TOPICS as $routed) {
            $before[$routed] = $this->probe()->totalEnd($routed);
        }

        $tester = $this->produce(3, $messageName);

        self::assertSame(3, substr_count($tester->getDisplay(), sprintf('produced %s → %s', $messageName, $topic)));
        foreach (self::ROUTED_TOPICS as $routed) {
            $expected = $routed === $topic ? 3 : 0;
            self::assertSame($expected, $this->probe()->totalEnd($routed) - $before[$routed], $routed);
        }
    }

    /**
     * @return iterable<string, array{string, string}>
     */
    public static function routes(): iterable
    {
        yield 'order.created → orders' => ['order.created', 'enet.ecommerce.orders'];
        yield 'payment.processed → payments' => ['payment.processed', 'enet.ecommerce.payments'];
        yield 'inventory.reserved → inventory' => ['inventory.reserved', 'enet.ecommerce.inventory'];
        yield 'order.audited → audit' => ['order.audited', 'enet.ecommerce.audit'];
        yield 'demo.order.evolved → demo' => ['demo.order.evolved', 'enet.demo.orders'];
    }

    public function testSimpleProfileProduces(): void
    {
        $before = $this->routedTotal();

        $tester = $this->produce(3, null, [
            '--profile' => 'simple',
        ]);

        self::assertStringContainsString('producer profile=producer.simple', $tester->getDisplay());
        self::assertSame(3, $this->routedTotal() - $before);
    }

    public function testSingleIdPoolKeepsOneKeyOnOnePartition(): void
    {
        $before = $this->probe()->endOffsets(self::ORDERS_TOPIC);

        $tester = $this->produce(6, 'order.created', [
            '--pool' => '1',
        ]);

        $keys = self::producedKeys($tester);
        self::assertCount(6, $keys);
        self::assertCount(1, array_unique($keys), 'a pool of 1 must reuse a single order id');

        $deltas = [];
        foreach ($this->probe()->endOffsets(self::ORDERS_TOPIC) as $partition => $end) {
            $deltas[$partition] = $end - ($before[$partition] ?? 0);
        }
        $touched = array_filter($deltas);
        self::assertSame([6], array_values($touched), 'one key must land all 6 messages on exactly one partition');
    }

    public function testThrottleIntervalDelaysSends(): void
    {
        $startedAt = microtime(true);

        $this->produce(3, 'order.created', [
            '--interval' => '100',
        ]);

        self::assertGreaterThanOrEqual(0.2, microtime(true) - $startedAt, 'two inter-send pauses of 100ms must show up in the wall clock');
    }

    public function testInvalidInputsAreRejectedBeforeProducing(): void
    {
        $before = $this->routedTotal();

        $cases = [
            'unknown profile' => [[
                '--count' => '1',
                '--profile' => 'bogus',
            ], 'Unknown profile'],
            'count below 1' => [[
                '--count' => '0',
            ], '--count must be >= 1'],
            'pool below 1' => [[
                '--count' => '1',
                '--pool' => '0',
            ], '--pool must be >= 1'],
            'unknown message name' => [[
                '--count' => '1',
                '--message-name' => 'order.nope',
            ], 'Unknown message name'],
        ];
        foreach ($cases as $label => [$input, $error]) {
            $tester = $this->runCommand('kafka:produce:sample', $input);

            self::assertSame(Command::INVALID, $tester->getStatusCode(), $label);
            self::assertStringContainsString($error, $tester->getDisplay(), $label);
        }

        self::assertSame(0, $this->routedTotal() - $before, 'validation must reject before anything reaches a topic');
    }

    private function routedTotal(): int
    {
        $total = 0;
        foreach (self::ROUTED_TOPICS as $topic) {
            $total += $this->probe()->totalEnd($topic);
        }

        return $total;
    }
}
