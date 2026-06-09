<?php

declare(strict_types=1);

namespace Workshop\App\Consumer;

/**
 * The MessageBus handler for the Block 4 schema-evolution event (OrderEvolvedDto):
 * instead of projecting to the database it prints the DTO's fields, one per line,
 * to make the consumer side legible. Whatever fields the read-model DTO declares
 * show up, so adding a property to the DTO — or switching the reader schema —
 * visibly changes what the consumer captured.
 *
 * It reflects the DTO's public properties, so it keeps working as you extend the
 * DTO mid-exercise. It writes through ConsoleWriter (a bus handler is a DI service
 * with no command output of its own); the command binds the real output at startup.
 * Pair it with `kafka:consume --print`, which dumps the raw decoded record (the
 * wire fields, pre-DTO) — together they show wire vs DTO side by side.
 */
#[AsMessageHandler]
final readonly class FieldPrintHandler
{
    public function __construct(
        private ConsoleWriter $console,
    ) {
    }

    public function __invoke(OrderEvolvedDto $dto): void
    {
        $fields = $this->fields($dto);
        $width = 0;
        foreach (array_keys($fields) as $name) {
            $width = max($width, \strlen($name));
        }

        foreach ($fields as $name => $value) {
            $this->console->writeln(sprintf('      %s = <info>%s</info>', str_pad($name, $width), $value));
        }
    }

    /**
     * @return array<string, string> public property name => rendered value
     */
    private function fields(object $dto): array
    {
        $fields = [];
        foreach ((new \ReflectionObject($dto))->getProperties(\ReflectionProperty::IS_PUBLIC) as $property) {
            $fields[$property->getName()] = $this->render($property->getValue($dto));
        }

        return $fields;
    }

    private function render(mixed $value): string
    {
        if (null === $value) {
            return '«null»';
        }
        if (\is_bool($value)) {
            return $value ? 'true' : 'false';
        }
        if (\is_scalar($value)) {
            return (string) $value;
        }
        if (\is_array($value)) {
            return '[' . implode(', ', array_map(fn (mixed $v): string => $this->render($v), $value)) . ']';
        }
        if (\is_object($value)) {
            // A nested DTO (e.g. Money, CustomerReference): render its public props
            // compactly on one line so the top-level field stays a single line.
            $parts = [];
            foreach ((new \ReflectionObject($value))->getProperties(\ReflectionProperty::IS_PUBLIC) as $property) {
                $parts[] = $property->getName() . ': ' . $this->render($property->getValue($value));
            }

            return '{ ' . implode(', ', $parts) . ' }';
        }

        return '?';
    }
}
