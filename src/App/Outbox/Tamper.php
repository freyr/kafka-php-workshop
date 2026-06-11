<?php

declare(strict_types=1);

namespace Workshop\App\Outbox;

/**
 * The Block 7 failure-injection kinds outbox:place can apply to a placement.
 * Two tampers, two distinct poison classes:
 *
 *  - Unframed: the payload ships as raw AVRO bytes WITHOUT the Confluent frame
 *    (magic byte + schema id) — the classic real-life incident of a producer
 *    using the wrong serializer (plain Avro, or JSON, instead of the wire
 *    format). The consumer routes the message, sees no frame, and can never
 *    decode it. POISON (deserialization), detected locally and
 *    deterministically — no registry involved.
 *  - Headerless: the payload is perfectly valid framed AVRO, but the message
 *    ships without the message-name header. The header is not AVRO — it is the
 *    envelope CONVENTION, and without it the record cannot be routed, ever.
 *    POISON (contract violation).
 *
 * Corrupting the BODY under a valid frame is deliberately NOT offered: it is
 * not poison at all — avro-php decodes garbage (even zero bytes) into the
 * schema's default values without throwing, the DTO hydrates the junk, and the
 * handler applies it. That silent-corruption failure mode is taught in the
 * block notes; it cannot be demoed as an error because no error ever surfaces.
 * (A broken schema ID inside an intact frame is the registry-flavored variant
 * of Unframed — a deterministic 404 on decode; taught, not coded.)
 */
enum Tamper
{
    case None;
    case Unframed;
    case Headerless;
}
