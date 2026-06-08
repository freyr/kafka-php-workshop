<?php

declare(strict_types=1);

namespace Workshop\Tests\Produce;

use PHPUnit\Framework\TestCase;
use Workshop\Produce\Message;
use Workshop\Produce\MessageNameResolver;
use Workshop\Produce\OrderCreated;

final class MessageNameResolverTest extends TestCase
{
    public function testResolvesNameFromAttribute(): void
    {
        $resolver = new MessageNameResolver();

        self::assertSame('order-created', $resolver->nameOf(OrderCreated::create('ord-1')));
    }

    public function testCachedResultIsStableAcrossCalls(): void
    {
        $resolver = new MessageNameResolver();
        $message = OrderCreated::create('ord-1');

        self::assertSame($resolver->nameOf($message), $resolver->nameOf($message));
    }

    public function testThrowsWhenAttributeMissing(): void
    {
        $message = new class extends Message {
            public function __construct()
            {
                parent::__construct('x', []);
            }
        };

        $this->expectException(\LogicException::class);
        (new MessageNameResolver())->nameOf($message);
    }
}
