<?php

declare(strict_types=1);

namespace Workshop\App\Consumer;

final readonly class ErrorDemoDto
{
    public function __construct(
        public string $id,
        public int $seq,
        public string $note,
    ) {
    }
}
