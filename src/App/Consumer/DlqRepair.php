<?php

declare(strict_types=1);

namespace Workshop\App\Consumer;

use Workshop\App\Producer\MessageRouting;
use Workshop\Kafka\Serde\ConfluentFrame;
use Workshop\Kafka\Serde\SchemaRegistryClient;

/**
 * The payload surgery behind kafka:dlq:replay's --fix-* options. The DLQ holds
 * messages that are broken IN THEMSELVES (handler trouble lives on the retry
 * topic, never here), so recovery is manual, per-message work: diagnose each
 * dead letter, then either repair its payload/headers and replay it, or drop
 * it. This service is the repair half — it owns the two fixes that match the
 * two poison classes:
 *
 *  - restore a missing message-name header (the convention-contract poison) —
 *    the operator supplies the name; nothing in the bytes can.
 *  - re-frame a payload that shipped as raw AVRO without the Confluent frame
 *    (the wrong-serializer poison) — the frame is rebuilt from the subject's
 *    latest registered schema id, on the operator's judgment that the raw body
 *    was written with that schema.
 *
 * repair() never guesses: each fix runs only when its flag is set AND its
 * defect is present, and anything still broken afterwards is reported so the
 * operator knows the message would just poison again.
 */
final readonly class DlqRepair
{
    public function __construct(
        private MessageRouting $routing,
        private SchemaRegistryClient $registry,
    ) {
    }

    /**
     * @param array<string, string> $headers the headers that will be replayed
     *                                       (diagnostics already stripped)
     *
     * @return array{payload: string, headers: array<string, string>, applied: list<string>, defects: list<string>}
     */
    public function repair(string $payload, array $headers, bool $fixFrame, ?string $fixMessageName): array
    {
        $applied = [];

        if (null !== $fixMessageName && '' === ($headers['message-name'] ?? '')) {
            $headers['message-name'] = $fixMessageName;
            $applied[] = sprintf('message-name header restored (%s)', $fixMessageName);
        }

        if ($fixFrame && ! ConfluentFrame::isFramed($payload)) {
            [$payload, $note] = $this->reframe($payload, $headers['message-name'] ?? '');
            if (null !== $note) {
                $applied[] = $note;
            }
        }

        return [
            'payload' => $payload,
            'headers' => $headers,
            'applied' => $applied,
            'defects' => $this->remainingDefects($payload, $headers),
        ];
    }

    /**
     * Rebuild the Confluent frame from the subject's latest registered schema id.
     * The route comes from the message name — without one (still headerless) the
     * writer schema cannot be identified and the payload is left as-is; the
     * defect report tells the operator to fix the name first.
     *
     * @return array{0: string, 1: ?string} the (possibly re-framed) payload and
     *                                      the applied-fix note
     */
    private function reframe(string $payload, string $name): array
    {
        if ('' === $name) {
            return [$payload, null];
        }

        try {
            $subject = $this->routing->for($name)->subject;
        } catch (\InvalidArgumentException) {
            return [$payload, null]; // no route → no subject to re-frame against
        }

        $schemaId = $this->registry->latestSchemaId($subject);
        if (null === $schemaId) {
            return [$payload, null]; // subject not registered → nothing to stamp
        }

        return [
            ConfluentFrame::prepend($schemaId, $payload),
            sprintf('re-framed against subject %s, schema id %d', $subject, $schemaId),
        ];
    }

    /**
     * What is STILL broken after the repairs ran — replaying such a message just
     * poisons it again, so the operator gets told before it happens.
     *
     * @param array<string, string> $headers
     *
     * @return list<string>
     */
    private function remainingDefects(string $payload, array $headers): array
    {
        $defects = [];
        if ('' === ($headers['message-name'] ?? '')) {
            $defects[] = 'no message-name header (use --fix-message-name=<name>)';
        }
        if (! ConfluentFrame::isFramed($payload)) {
            $defects[] = 'payload is not Confluent-framed (use --fix-frame; needs a message-name to resolve the subject)';
        }

        return $defects;
    }
}
