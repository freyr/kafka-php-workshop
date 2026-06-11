<?php

declare(strict_types=1);

namespace Workshop\Tests\App\Consumer;

use GuzzleHttp\Client;
use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Psr7\Response;
use PHPUnit\Framework\TestCase;
use Workshop\App\Consumer\DlqRepair;
use Workshop\App\Producer\MessageRouting;
use Workshop\Kafka\Serde\SchemaRegistryClient;

final class DlqRepairTest extends TestCase
{
    public function testRestoresAMissingMessageNameHeader(): void
    {
        $result = $this->repair()->repair("\x00\x00\x00\x00\x06valid", [
            'event-id' => 'e1',
        ], false, 'error.demo');

        self::assertSame('error.demo', $result['headers']['message-name']);
        self::assertSame(['message-name header restored (error.demo)'], $result['applied']);
        self::assertSame([], $result['defects']);
    }

    public function testNeverOverwritesAnExistingMessageName(): void
    {
        $result = $this->repair()->repair("\x00\x00\x00\x00\x06valid", [
            'message-name' => 'order.created',
        ], false, 'error.demo');

        self::assertSame('order.created', $result['headers']['message-name']);
        self::assertSame([], $result['applied']);
    }

    public function testReframesARawPayloadAgainstTheSubjectsLatestSchemaId(): void
    {
        $result = $this->repair(registryReturnsId: 6)->repair('raw-avro-body', [
            'message-name' => 'error.demo',
        ], true, null);

        self::assertSame("\x00\x00\x00\x00\x06raw-avro-body", $result['payload']);
        self::assertSame(['re-framed against subject com.ecommerce.demo.ErrorDemo, schema id 6'], $result['applied']);
        self::assertSame([], $result['defects']);
    }

    public function testFrameRepairNeedsAMessageNameToResolveTheSubject(): void
    {
        $result = $this->repair()->repair('raw-avro-body', [], true, null);

        self::assertSame('raw-avro-body', $result['payload'], 'no name → no subject → nothing to re-frame against');
        self::assertSame([], $result['applied']);
        self::assertCount(2, $result['defects'], 'both defects reported: the missing name and the missing frame');
    }

    public function testBothFixesComposeTheNameUnlocksTheReframe(): void
    {
        $result = $this->repair(registryReturnsId: 6)->repair('raw-avro-body', [
            'event-id' => 'e1',
        ], true, 'error.demo');

        self::assertSame('error.demo', $result['headers']['message-name']);
        self::assertSame("\x00\x00\x00\x00\x06raw-avro-body", $result['payload']);
        self::assertCount(2, $result['applied']);
        self::assertSame([], $result['defects']);
    }

    public function testAFramedPayloadIsNeverTouched(): void
    {
        $result = $this->repair()->repair("\x00\x00\x00\x00\x06valid", [
            'message-name' => 'error.demo',
        ], true, null);

        self::assertSame("\x00\x00\x00\x00\x06valid", $result['payload']);
        self::assertSame([], $result['applied']);
    }

    public function testRemainingDefectsAreReportedWhenNoFixIsRequested(): void
    {
        $result = $this->repair()->repair('raw-avro-body', [], false, null);

        self::assertCount(2, $result['defects']);
        self::assertStringContainsString('no message-name header', $result['defects'][0]);
        self::assertStringContainsString('not Confluent-framed', $result['defects'][1]);
    }

    private function repair(?int $registryReturnsId = null): DlqRepair
    {
        $mock = new MockHandler(null === $registryReturnsId ? [] : [
            new Response(200, [], json_encode([
                'id' => $registryReturnsId,
                'version' => 1,
                'schema' => '{}',
            ], JSON_THROW_ON_ERROR)),
        ]);

        return new DlqRepair(
            new MessageRouting([
                'error.demo' => [
                    'topic' => 'enet.ecommerce.outbox.ErrorDemo',
                    'subject' => 'com.ecommerce.demo.ErrorDemo',
                ],
            ]),
            new SchemaRegistryClient(new Client([
                'handler' => HandlerStack::create($mock),
            ])),
        );
    }
}
