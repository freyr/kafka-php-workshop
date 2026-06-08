<?php

declare(strict_types=1);

namespace Workshop\Produce;

/**
 * The wire identity of one message name: which topic it is produced to, the
 * Schema Registry subject it registers under (RecordNameStrategy), and the path
 * to its AVRO schema.
 */
final readonly class Route
{
    public function __construct(
        public string $topic,
        public string $subject,
        public string $schemaPath,
    ) {
    }

    public function schemaJson(): string
    {
        $json = file_get_contents($this->schemaPath);
        if (false === $json) {
            throw new \RuntimeException("Unable to read schema file: {$this->schemaPath}");
        }

        return $json;
    }
}
