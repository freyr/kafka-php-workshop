<?php

declare(strict_types=1);

namespace Workshop\Kernel;

use Enqueue\RdKafka\RdKafkaMessage;
use Enqueue\RdKafka\Serializer;

/**
 * Keep payloads on the wire as plain strings instead of the JsonSerializer
 * envelope enqueue/rdkafka uses by default. Workshop messages are
 * deliberately raw until AVRO is introduced in Block 04.
 */
final class RawStringSerializer implements Serializer
{
    public function toString(RdKafkaMessage $message): string
    {
        return $message->getBody();
    }

    public function toMessage(string $string): RdKafkaMessage
    {
        return new RdKafkaMessage($string);
    }
}
