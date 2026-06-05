<?php

declare(strict_types=1);

namespace Workshop\Console;

use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Helper\Table;
use Symfony\Component\Console\Helper\TableSeparator;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Workshop\Kernel\KafkaSetting;
use Workshop\Kernel\KafkaTuning;

#[AsCommand(
    name: 'config:show',
    description: 'Block 8: print the workshop\'s recommended producer/consumer librdkafka config — value, default, and why — so every non-default value can be defended. No broker contact.',
)]
final class ConfigShowCommand extends Command
{
    public function __construct(
        private readonly KafkaTuning $tuning,
    ) {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this
            ->addOption('producer', null, InputOption::VALUE_NONE, 'Show only the producer config')
            ->addOption('consumer', null, InputOption::VALUE_NONE, 'Show only the consumer config');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $onlyProducer = (bool) $input->getOption('producer');
        $onlyConsumer = (bool) $input->getOption('consumer');
        // No flag = show both; either flag narrows to that role.
        $showProducer = $onlyProducer || ! $onlyConsumer;
        $showConsumer = $onlyConsumer || ! $onlyProducer;

        if ($showProducer) {
            $this->renderRole($output, 'PRODUCER', $this->tuning->producer());
        }

        if ($showProducer && $showConsumer) {
            $output->writeln('');
        }

        if ($showConsumer) {
            $this->renderRole($output, 'CONSUMER', $this->tuning->consumer());
        }

        $output->writeln('');
        $output->writeln('<comment>bold value = differs from the librdkafka default. Inject via KafkaContextFactory::forProducer($overrides) / forConsumer($group, $overrides).</comment>');

        return Command::SUCCESS;
    }

    /**
     * @param list<KafkaSetting> $settings
     */
    private function renderRole(OutputInterface $output, string $role, array $settings): void
    {
        $output->writeln(sprintf('<info>%s — recommended production config</info>', $role));

        $table = new Table($output);
        $table->setHeaders(['group', 'setting', 'value', 'default', 'why']);

        $previousGroup = null;
        foreach ($settings as $setting) {
            if (null !== $previousGroup && $setting->group !== $previousGroup) {
                $table->addRow(new TableSeparator());
            }

            $value = $setting->isNonDefault()
                ? sprintf('<options=bold>%s</>', $setting->value)
                : $setting->value;

            $table->addRow([
                $setting->group === $previousGroup ? '' : $setting->group,
                $setting->key,
                $value,
                $setting->default,
                $this->wrap($setting->why, 60),
            ]);

            $previousGroup = $setting->group;
        }

        $table->render();
    }

    /**
     * Soft-wrap the rationale so the table column stays readable in a terminal.
     */
    private function wrap(string $text, int $width): string
    {
        return wordwrap($text, $width, "\n", true);
    }
}
