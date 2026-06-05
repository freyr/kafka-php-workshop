<?php

declare(strict_types=1);

namespace Workshop\Kernel;

/**
 * One librdkafka configuration property the workshop recommends, paired with the
 * default it overrides and a one-line reason. The whole point of Block 8 is that
 * the team can *defend* every non-default value, so the rationale travels with
 * the value rather than living in a slide.
 */
final readonly class KafkaSetting
{
    public function __construct(
        public string $group,
        public string $key,
        public string $value,
        public string $default,
        public string $why,
    ) {
    }

    public function isNonDefault(): bool
    {
        return $this->value !== $this->default;
    }
}
