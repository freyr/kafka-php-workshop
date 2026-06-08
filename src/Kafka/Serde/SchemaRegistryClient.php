<?php

declare(strict_types=1);

namespace Workshop\Kafka\Serde;

use GuzzleHttp\Client;
use GuzzleHttp\Exception\ConnectException;
use GuzzleHttp\Exception\GuzzleException;
use Psr\Http\Message\RequestInterface;

use function FlixTech\SchemaRegistryApi\Requests\allSubjectVersionsRequest;
use function FlixTech\SchemaRegistryApi\Requests\checkSchemaCompatibilityAgainstVersionRequest;
use function FlixTech\SchemaRegistryApi\Requests\registerNewSchemaVersionWithSubjectRequest;

/**
 * Thin Schema Registry REST client. Wraps the flix-tech PSR-7 request builders
 * with a Guzzle client so the workshop can drive the three operations that matter
 * for schema evolution — registering a schema (the explicit, out-of-band path a
 * production deploy/CI runs), the pre-registration compatibility check (the CI
 * gate), and listing a subject's registered versions — without hand-rolling curl.
 * Schema *encoding* goes through {@see AvroSerializer}; this client never touches
 * the wire format.
 */
final readonly class SchemaRegistryClient
{
    public function __construct(
        private Client $http,
    ) {
    }

    /**
     * Register $schemaJson as a version of $subject and return the globally unique
     * schema id the registry assigns. The registry enforces the subject's
     * compatibility level server-side — a breaking schema is rejected (409) and
     * never stored. Registration is idempotent: re-registering an identical schema
     * returns the existing id without minting a new version.
     *
     * This is the production registration path — explicit and out of band — that
     * replaces auto-registering schemas on first produce.
     *
     * @throws GuzzleException
     * @throws SchemaRegistrationException when the registry refuses the schema
     */
    public function register(string $subject, string $schemaJson): int
    {
        $response = $this->send(registerNewSchemaVersionWithSubjectRequest($schemaJson, $subject));

        if (200 !== $response['status']) {
            throw SchemaRegistrationException::rejected($subject, $response['status'], $response['body']);
        }

        $body = json_decode($response['body'], true);
        if (! \is_array($body) || ! \is_int($body['id'] ?? null)) {
            throw SchemaRegistrationException::unexpectedBody($subject, $response['body']);
        }

        return $body['id'];
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
     *
     * @throws GuzzleException
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
     *
     * @throws GuzzleException
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
     * @return array{status: int, body: string}
     *
     * @throws GuzzleException
     */
    private function send(RequestInterface $request): array
    {
        try {
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
