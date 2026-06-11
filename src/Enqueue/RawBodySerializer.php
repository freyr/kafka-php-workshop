<?php

declare(strict_types=1);

namespace Workshop\Enqueue;

use Enqueue\RdKafka\RdKafkaMessage;
use Enqueue\RdKafka\Serializer;

/**
 * A pass-through enqueue serializer: the message body goes on the wire verbatim
 * and comes back verbatim. Enqueue's default JsonSerializer wraps every record in
 * its own {body, properties, headers} JSON envelope — fine when enqueue owns both
 * ends, fatal on a shared topic: it would bury the Confluent-framed AVRO bytes
 * (and the outbox JSON) inside a wrapper no other consumer understands.
 *
 * A production enqueue setup that talks to Schema Registry therefore swaps in a
 * raw serializer like this one and carries metadata in real Kafka headers
 * (message-name, event-id) instead of in the body — the records this layer writes
 * stay byte-identical to the pure-rdkafka producers', so either side can read the
 * other's topics.
 */
final class RawBodySerializer implements Serializer
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
