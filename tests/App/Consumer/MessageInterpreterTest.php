<?php

declare(strict_types=1);

namespace Workshop\Tests\App\Consumer;

use PHPUnit\Framework\TestCase;
use Workshop\App\Consumer\DtoRouting;
use Workshop\App\Consumer\MessageDenormalizer;
use Workshop\App\Consumer\MessageInterpreter;
use Workshop\App\Consumer\OrderCreatedDto;
use Workshop\App\Producer\Message;
use Workshop\Kafka\Runtime\PoisonMessageException;
use Workshop\Kafka\Serde\MessageSerializer;

final class MessageInterpreterTest extends TestCase
{
    private const array ENVELOPE = [
        'metadata' => [
            'event_id' => 'meta-id',
            'timestamp' => 123,
        ],
        'order_id' => 'ord-1',
        'customer' => [
            'display_name' => 'Jane',
        ],
        'totals' => [
            'total' => [
                'amount_cents' => 500,
            ],
        ],
    ];

    public function testInterpretsARoutedRecordIntoItsDto(): void
    {
        $consumed = $this->interpreter(self::ENVELOPE)->interpret(
            $this->message([
                'message-name' => 'order.created',
                'event-id' => 'hdr-id',
            ], partition: 2, offset: 7),
        );

        self::assertNotNull($consumed);
        self::assertSame('order.created', $consumed->name);
        self::assertSame('hdr-id', $consumed->eventId, 'the header id wins over the envelope id');
        self::assertSame(2, $consumed->partition);
        self::assertSame(7, $consumed->offset);
        self::assertInstanceOf(OrderCreatedDto::class, $consumed->dto);
        self::assertSame('ord-1', $consumed->dto->orderId);
        self::assertSame('Jane', $consumed->dto->customer->displayName);
        self::assertSame(500, $consumed->dto->totals->total->amountCents);
    }

    public function testDecodeExposesTheRawPayloadWithUnmappedFieldsAndStripsMetadata(): void
    {
        // An evolved record carries a wire field the OrderCreatedDto does not map.
        $envelope = self::ENVELOPE + [
            'loyalty_tier' => 'gold',
        ];

        $decoded = $this->interpreter($envelope)->decode(
            $this->message([
                'message-name' => 'order.created',
                'event-id' => 'hdr-id',
            ], partition: 1, offset: 3),
        );

        self::assertNotNull($decoded);
        self::assertSame('order.created', $decoded->name);
        self::assertSame('hdr-id', $decoded->eventId);
        self::assertSame(OrderCreatedDto::class, $decoded->dtoClass);
        self::assertSame(1, $decoded->partition);
        self::assertSame(3, $decoded->offset);
        self::assertArrayNotHasKey('metadata', $decoded->payload, 'the reserved envelope is stripped from the raw payload');
        self::assertSame('gold', $decoded->payload['loyalty_tier'] ?? null, 'an unmapped wire field survives the decode');
        self::assertSame('ord-1', $decoded->payload['order_id'] ?? null);
    }

    public function testFallsBackToTheEnvelopeEventIdWhenTheHeaderIsAbsent(): void
    {
        $consumed = $this->interpreter(self::ENVELOPE)->interpret(
            $this->message([
                'message-name' => 'order.created',
            ]),
        );

        self::assertNotNull($consumed);
        self::assertSame('meta-id', $consumed->eventId);
    }

    public function testUnhandledNameIsSkippedWithoutDecoding(): void
    {
        $decodeCalled = false;
        $interpreter = new MessageInterpreter(
            new DtoRouting([
                'order.created' => OrderCreatedDto::class,
            ]),
            $this->serializer(self::ENVELOPE, $decodeCalled),
            new MessageDenormalizer(),
        );

        $consumed = $interpreter->interpret($this->message([
            'message-name' => 'order.unknown',
            'event-id' => 'x',
        ]));

        self::assertNull($consumed);
        self::assertFalse($decodeCalled, 'an unhandled type must not be decoded');
    }

    public function testNonAvroBytesAreSkipped(): void
    {
        $consumed = $this->interpreter(null)->interpret(
            $this->message([
                'message-name' => 'order.created',
                'event-id' => 'x',
            ]),
        );

        self::assertNull($consumed);
    }

    public function testRecordWithNoResolvableIdIsSkipped(): void
    {
        $envelopeWithoutId = [
            'order_id' => 'ord-1',
            'customer' => [
                'display_name' => 'Jane',
            ],
            'totals' => [
                'total' => [
                    'amount_cents' => 500,
                ],
            ],
        ];

        $consumed = $this->interpreter($envelopeWithoutId)->interpret(
            $this->message([
                'message-name' => 'order.created',
            ]),
        );

        self::assertNull($consumed);
    }

    public function testPoisonGateThrowsOnADecodeFailureOfARoutedType(): void
    {
        $this->expectException(PoisonMessageException::class);
        $this->expectExceptionMessageMatches('/AVRO decode failed/');

        $this->throwingInterpreter()->decode(
            $this->message([
                'message-name' => 'order.created',
                'event-id' => 'x',
            ]),
            poisonGate: true,
        );
    }

    public function testPoisonGateThrowsOnNonAvroBytesOfARoutedType(): void
    {
        $this->expectException(PoisonMessageException::class);
        $this->expectExceptionMessageMatches('/not Confluent-framed/');

        $this->interpreter(null)->decode(
            $this->message([
                'message-name' => 'order.created',
                'event-id' => 'x',
            ]),
            poisonGate: true,
        );
    }

    public function testPoisonGateThrowsOnAMissingEventId(): void
    {
        $this->expectException(PoisonMessageException::class);
        $this->expectExceptionMessageMatches('/no resolvable event id/');

        $this->interpreter([
            'order_id' => 'ord-1',
        ])->decode(
            $this->message([
                'message-name' => 'order.created',
            ]),
            poisonGate: true,
        );
    }

    public function testPoisonGateStillSkipsAnUnroutedNameSilently(): void
    {
        $decoded = $this->throwingInterpreter()->decode(
            $this->message([
                'message-name' => 'somebody.elses.event',
                'event-id' => 'x',
            ]),
            poisonGate: true,
        );

        self::assertNull($decoded, 'a type this consumer does not handle is never poison — shared-topic tolerance');
    }

    public function testWithoutThePoisonGateADecodeFailureStaysANullSkip(): void
    {
        $decoded = $this->throwingInterpreter()->decode(
            $this->message([
                'message-name' => 'order.created',
                'event-id' => 'x',
            ]),
        );

        self::assertNull($decoded, 'the tolerant default contract is unchanged');
    }

    /**
     * @param array<string, mixed>|null $decoded
     */
    private function interpreter(?array $decoded): MessageInterpreter
    {
        $unused = false;

        return new MessageInterpreter(
            new DtoRouting([
                'order.created' => OrderCreatedDto::class,
            ]),
            $this->serializer($decoded, $unused),
            new MessageDenormalizer(),
        );
    }

    /**
     * An interpreter whose serializer throws on every decode — the broken-AVRO
     * (poison) case.
     */
    private function throwingInterpreter(): MessageInterpreter
    {
        $serializer = new class implements MessageSerializer {
            public function encode(Message $payload): string
            {
                return '';
            }

            public function decode(string $bytes, ?\AvroSchema $readerSchema = null): mixed
            {
                throw new \RuntimeException('malformed body');
            }
        };

        return new MessageInterpreter(
            new DtoRouting([
                'order.created' => OrderCreatedDto::class,
            ]),
            $serializer,
            new MessageDenormalizer(),
        );
    }

    /**
     * @param array<string, mixed>|null $decoded
     */
    private function serializer(?array $decoded, bool &$decodeCalled): MessageSerializer
    {
        return new class($decoded, $decodeCalled) implements MessageSerializer {
            /**
             * @param array<string, mixed>|null $decoded
             */
            public function __construct(
                private readonly ?array $decoded,
                private bool &$decodeCalled,
            ) {
            }

            public function encode(Message $payload): string
            {
                return '';
            }

            public function decode(string $bytes, ?\AvroSchema $readerSchema = null): mixed
            {
                $this->decodeCalled = true;

                return $this->decoded;
            }
        };
    }

    /**
     * @param array<string, string> $headers
     */
    private function message(array $headers, int $partition = 0, int $offset = 0): \RdKafka\Message
    {
        $message = new \RdKafka\Message();
        $message->err = RD_KAFKA_RESP_ERR_NO_ERROR;
        $message->payload = 'bytes';
        $message->headers = $headers;
        $message->partition = $partition;
        $message->offset = $offset;

        return $message;
    }
}
