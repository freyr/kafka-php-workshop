<?php

declare(strict_types=1);

namespace Workshop\App\Consumer;

use Symfony\Component\Console\Output\OutputInterface;

/**
 * A consumer handler that prints a DTO's fields, one per line, instead of
 * projecting it. The Block 4 schema-evolution exercise uses it (via
 * `kafka:consume --print`) to make the consumer side legible: whatever fields the
 * read-model DTO declares show up, so adding a property to the DTO — or switching
 * the reader schema — visibly changes what the consumer sees.
 *
 * It reflects the DTO's public properties, so it needs no per-type knowledge and
 * prints any DTO, including one you extend mid-exercise. Constructed per run with
 * the command's output (not a DI service — it has nowhere to write otherwise).
 */
final readonly class FieldPrintHandler implements DtoHandler
{
    public function __construct(
        private OutputInterface $output,
    ) {
    }

    public function handle(object $dto): void
    {
        $fields = $this->fields($dto);
        $width = 0;
        foreach (array_keys($fields) as $name) {
            $width = max($width, \strlen($name));
        }

        foreach ($fields as $name => $value) {
            $this->output->writeln(sprintf('      %s = <info>%s</info>', str_pad($name, $width), $value));
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
