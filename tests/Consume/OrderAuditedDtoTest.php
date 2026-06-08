<?php

declare(strict_types=1);

namespace Workshop\Tests\Consume;

use PHPUnit\Framework\TestCase;
use Workshop\Consume\MessageDenormalizer;
use Workshop\Consume\OrderAuditedDto;

final class OrderAuditedDtoTest extends TestCase
{
    public function testDenormalizesSnakeCasePayloadAndIgnoresEnvelopeRemnants(): void
    {
        $payload = [
            'order_id' => 'ord-123',
            'action' => 'observed',
        ];

        $dto = (new MessageDenormalizer())->denormalize($payload, OrderAuditedDto::class);

        self::assertInstanceOf(OrderAuditedDto::class, $dto);
        self::assertSame('ord-123', $dto->orderId);
        self::assertSame('observed', $dto->action);
    }
}
