<?php

declare(strict_types=1);

namespace Workshop\Tests\App\Consumer;

use Doctrine\DBAL\Connection;
use PHPUnit\Framework\TestCase;
use Workshop\App\Consumer\ConsumedMessage;
use Workshop\App\Consumer\EventDedup;
use Workshop\App\Consumer\IdempotencyMiddleware;

final class IdempotencyMiddlewareTest extends TestCase
{
    public function testAlreadySeenEventSkipsTheHandlerAndRecordsNothing(): void
    {
        $connection = $this->createMock(Connection::class);
        $connection->method('fetchOne')->willReturn(1); // a row exists → seen
        $connection->expects(self::never())->method('insert');

        $ran = false;
        $next = static function () use (&$ran): void {
            $ran = true;
        };

        new IdempotencyMiddleware(new EventDedup($connection))->handle($this->message(), $next);

        self::assertFalse($ran, 'a seen event must not reach the handler');
    }

    public function testUnseenEventRunsTheHandlerThenRecordsTheId(): void
    {
        $connection = $this->createMock(Connection::class);
        $connection->method('fetchOne')->willReturn(false); // no row → unseen
        $connection->expects(self::once())
            ->method('insert')
            ->with('processed_events', self::callback(static fn (array $data): bool => 'e1' === $data['event_id']));

        $ran = false;
        $next = static function () use (&$ran): void {
            $ran = true;
        };

        new IdempotencyMiddleware(new EventDedup($connection))->handle($this->message(), $next);

        self::assertTrue($ran, 'an unseen event must reach the handler');
    }

    private function message(): ConsumedMessage
    {
        return new ConsumedMessage('e1', 'order.created', new \stdClass(), 0, 0);
    }
}
