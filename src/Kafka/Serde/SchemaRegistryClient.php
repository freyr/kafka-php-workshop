<?php

declare(strict_types=1);

namespace Workshop\Kafka\Serde;

use GuzzleHttp\Client;
use GuzzleHttp\Exception\ConnectException;

use function FlixTech\SchemaRegistryApi\Requests\allSubjectVersionsRequest;
use function FlixTech\SchemaRegistryApi\Requests\checkSchemaCompatibilityAgainstVersionRequest;

/**
 * Thin Schema Registry REST client for the Block 4 tooling demos. Wraps the
 * flix-tech PSR-7 request builders with a Guzzle client so the workshop can
 * show the two operations that matter for schema evolution — the
 * pre-registration compatibility check (the CI gate) and listing a subject's
 * registered versions — without hand-rolling curl. Schema *encoding* still goes
 * through {@see AvroEnvelopeSerializer}; this client never touches the wire format.
 */
final readonly class SchemaRegistryClient
{
    private Client $http;

    public function __construct(string $schemaRegistryUrl)
    {
        // http_errors=false so we can inspect 404 (subject not registered yet)
        // instead of catching exceptions for the expected "first version" case.
        $this->http = new Client([
            'base_uri' => rtrim($schemaRegistryUrl, '/') . '/',
            'http_errors' => false,
        ]);
    }

    /**
     * Check whether $schemaJson is compatible with the latest registered version
     * of $subject, under the subject's (or global) compatibility level. This is
     * the same check a CI pipeline runs before merging a schema change.
     *
     * @return array{compatible: bool, firstVersion: bool} firstVersion=true when
     *                                                     the subject has no
     *                                                     versions yet, so there
     *                                                     is nothing to check
     */
    public function checkCompatibility(string $subject, string $schemaJson): array
    {
        $response = $this->send(checkSchemaCompatibilityAgainstVersionRequest($schemaJson, $subject, 'latest'));
        $status = $response['status'];

        // 404 = subject (or version) not found — the first schema for a subject
        // is always accepted; there is no prior version to break.
        if (404 === $status) {
            return [
                'compatible' => true,
                'firstVersion' => true,
            ];
        }

        $body = json_decode($response['body'], true);

        return [
            'compatible' => \is_array($body) && true === ($body['is_compatible'] ?? false),
            'firstVersion' => false,
        ];
    }

    /**
     * List the registered version numbers for a subject, oldest first.
     *
     * @return int[] empty when the subject is not registered yet
     */
    public function versions(string $subject): array
    {
        $response = $this->send(allSubjectVersionsRequest($subject));

        if (404 === $response['status']) {
            return [];
        }

        $body = json_decode($response['body'], true);

        return \is_array($body) ? array_map(static fn (mixed $v): int => is_numeric($v) ? (int) $v : 0, $body) : [];
    }

    /**
     * @param \Psr\Http\Message\RequestInterface $request
     *
     * @return array{status: int, body: string}
     */
    private function send(object $request): array
    {
        try {
            /** @var \Psr\Http\Message\RequestInterface $request */
            $response = $this->http->send($request);
        } catch (ConnectException $e) {
            throw new \RuntimeException('Schema Registry is unreachable. Is the stack up? Try `make create`.', previous: $e);
        }

        return [
            'status' => $response->getStatusCode(),
            'body' => (string) $response->getBody(),
        ];
    }
}
