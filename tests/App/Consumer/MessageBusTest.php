<?php

declare(strict_types=1);

namespace Workshop\Tests\App\Consumer;

use Doctrine\DBAL\Connection;
use PHPUnit\Framework\TestCase;
use Symfony\Component\DependencyInjection\ServiceLocator;
use Workshop\App\Consumer\ConsumedMessage;
use Workshop\App\Consumer\CustomerRefDto;
use Workshop\App\Consumer\EventDedup;
use Workshop\App\Consumer\IdempotencyMiddleware;
use Workshop\App\Consumer\MessageBus;
use Workshop\App\Consumer\MoneyDto;
use Workshop\App\Consumer\OrderAuditedDto;
use Workshop\App\Consumer\OrderCreatedDto;
use Workshop\App\Consumer\OrderTotalsDto;
use Workshop\App\Consumer\TransactionMiddleware;

final class MessageBusTest extends TestCase
{
    public function testRoutesADtoToItsRegisteredHandler(): void
    {
        $spy = $this->spy();
        $bus = $this->bus([
            OrderCreatedDto::class => static fn (): object => $spy,
        ]);

        $dto = new OrderCreatedDto('ord-1', new CustomerRefDto('Jane'), new OrderTotalsDto(new MoneyDto(500)));
        $bus->dispatch(new ConsumedMessage('e1', 'order.created', $dto, 0, 0), idempotent: false);

        self::assertSame([$dto], $spy->handled);
    }

    public function testADtoWithNoHandlerIsANoOp(): void
    {
        $spy = $this->spy();
        $bus = $this->bus([
            OrderCreatedDto::class => static fn (): object => $spy,
        ]);

        $bus->dispatch(
            new ConsumedMessage('e2', 'order.audited', new OrderAuditedDto('ord-2', 'looked'), 0, 0),
            idempotent: false,
        );

        self::assertSame([], $spy->handled, 'an unmapped DTO routes nowhere');
    }

    /**
     * @return object{handled: list<object>}
     */
    private function spy(): object
    {
        return new class {
            /**
             * @var list<object>
             */
            public array $handled = [];

            public function __invoke(object $dto): void
            {
                $this->handled[] = $dto;
            }
        };
    }

    /**
     * @param array<class-string, callable(): object> $handlers
     */
    private function bus(array $handlers): MessageBus
    {
        // Middleware is constructed but never exercised: dispatch is called with
        // idempotent=false, so the bus folds no middleware around the handler.
        $connection = $this->createStub(Connection::class);

        return new MessageBus(
            new ServiceLocator($handlers),
            new TransactionMiddleware($connection),
            new IdempotencyMiddleware(new EventDedup($connection)),
        );
    }
}
