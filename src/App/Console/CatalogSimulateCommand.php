<?php

declare(strict_types=1);

namespace Workshop\App\Console;

use Doctrine\DBAL\Exception\DriverException;
use FlixTech\SchemaRegistryApi\Exception\SchemaRegistryException;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Uid\Uuid;
use Workshop\App\Catalog\CatalogChangePlacer;
use Workshop\App\Catalog\ProjectionChange;

#[AsCommand(
    name: 'catalog:simulate',
    description: 'Simulate ProductCatalog changes: each one appends a full-state projection-change event (AVRO wire bytes) to product_catalog_state_change — no Kafka client anywhere.',
)]
final class CatalogSimulateCommand extends Command
{
    /**
     * The demo catalog: a small fixed pool, so repeated runs hit the same SKUs
     * and the projection visibly flips from first-sight INSERT to UPDATE
     * (watch updated_at move while created_at stays).
     */
    private const array CATALOG = [
        'SKU-ESPRESSO-1KG' => 'Espresso Beans 1kg',
        'SKU-V60-DRIPPER' => 'V60 Ceramic Dripper',
        'SKU-AEROPRESS-GO' => 'AeroPress Go',
        'SKU-GRINDER-MINI' => 'Hand Grinder Mini',
        'SKU-KETTLE-GOOSE' => 'Gooseneck Kettle 1L',
        'SKU-SCALE-01' => 'Brewing Scale 0.1g',
        'SKU-MUG-DOUBLE' => 'Double-Wall Mug 350ml',
        'SKU-FILTERS-100' => 'Paper Filters x100',
    ];

    public function __construct(
        private readonly CatalogChangePlacer $placer,
    ) {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this
            ->addOption('count', 'c', InputOption::VALUE_REQUIRED, 'How many product changes to place against the fixed catalog pool; default: 5', '5')
            ->addOption('new', null, InputOption::VALUE_REQUIRED, 'Additionally mint this many brand-new products (fresh SKUs — guaranteed INSERTs on the projection side); default: 0', '0')
            ->addOption('interval', null, InputOption::VALUE_REQUIRED, 'Milliseconds to pause between changes; default: 250', '250');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $count = Input::int($input, 'count');
        $new = Input::int($input, 'new');
        $intervalMs = Input::int($input, 'interval');

        if ($count < 0 || $new < 0 || 0 === $count + $new) {
            $output->writeln('<error>--count and --new must be >= 0, and together at least 1.</error>');

            return Command::INVALID;
        }

        $changes = $this->changes($count, $new);
        $total = count($changes);

        foreach ($changes as $i => [$sku, $name, $isNew]) {
            // Full state every time: price re-rolled, margin 10-40% of it. A real
            // catalog would read current state from its product table; the demo
            // re-rolls so every event visibly changes the projection row.
            $price = random_int(500, 15_000);
            $margin = intdiv($price * random_int(10, 40), 100);
            $change = ProjectionChange::create($sku, $name, $price, $margin);

            try {
                $this->placer->place($change);
            } catch (SchemaRegistryException) {
                $output->writeln('<error>No schema registered for catalog.projection_change.</error>');
                $output->writeln('The AVRO encode validates against the registry — register first, then simulate:');
                $output->writeln('  <comment>bin/console kafka:schema:register catalog.projection_change</comment>');

                return Command::FAILURE;
            } catch (DriverException $e) {
                $output->writeln(sprintf('<error>%s</error>', $e->getMessage()));
                $output->writeln('Is the demo store provisioned? Provision it with:');
                $output->writeln('  <comment>bin/console catalog:setup</comment>');

                return Command::FAILURE;
            }

            $output->writeln(sprintf(
                'placed <info>catalog.projection_change</info>%s sku=%s name="%s" price=%s margin=%s → state-change id=%s',
                $isNew ? ' <comment>(new product)</comment>' : '',
                $sku,
                $name,
                self::pln($price),
                self::pln($margin),
                $change->eventId(),
            ));

            if ($intervalMs > 0 && $i < $total - 1) {
                usleep($intervalMs * 1000);
            }
        }

        $output->writeln(sprintf(
            '<info>done</info> — %d full-state event(s) appended. Kafka was never contacted: Debezium tails the binlog, the JDBC sink upserts products_projection. Watch it land: <comment>make catalog-projection</comment>',
            $total,
        ));

        return Command::SUCCESS;
    }

    /**
     * The run's change list: --new brand-new SKUs first (guaranteed projection
     * INSERTs), then --count changes against the fixed pool (mostly UPDATEs).
     *
     * @return list<array{string, string, bool}> [sku, name, isNew]
     */
    private function changes(int $count, int $new): array
    {
        $changes = [];
        for ($i = 0; $i < $new; ++$i) {
            $suffix = strtoupper(substr(Uuid::v4()->toRfc4122(), 0, 8));
            $changes[] = ['SKU-NEW-' . $suffix, 'Limited Edition ' . $suffix, true];
        }

        $skus = array_keys(self::CATALOG);
        for ($i = 0; $i < $count; ++$i) {
            $sku = $skus[array_rand($skus)];
            $changes[] = [$sku, self::CATALOG[$sku], false];
        }

        return $changes;
    }

    private static function pln(int $cents): string
    {
        return sprintf('%d.%02d PLN', intdiv($cents, 100), $cents % 100);
    }
}
