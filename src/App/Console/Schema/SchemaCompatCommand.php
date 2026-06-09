<?php

declare(strict_types=1);

namespace Workshop\App\Console\Schema;

use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use Workshop\App\Console\Input;
use Workshop\App\Producer\MessageRouting;
use Workshop\Kafka\Serde\SchemaRegistrationException;
use Workshop\Kafka\Serde\SchemaRegistryClient;

#[AsCommand(
    name: 'kafka:schema:compat',
    description: 'Show or set the compatibility level of an event subject. The lever for the evolution exercise: switch a subject to BACKWARD_TRANSITIVE so the registry checks a candidate against EVERY past version, not just the latest.',
)]
final class SchemaCompatCommand extends Command
{
    private const array LEVELS = [
        'BACKWARD', 'BACKWARD_TRANSITIVE',
        'FORWARD', 'FORWARD_TRANSITIVE',
        'FULL', 'FULL_TRANSITIVE',
        'NONE',
    ];

    public function __construct(
        private readonly SchemaRegistryClient $registry,
        private readonly MessageRouting $routing,
    ) {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this
            ->addArgument('type', InputArgument::REQUIRED, implode(' | ', $this->routing->names()))
            ->addArgument('level', InputArgument::OPTIONAL, 'Set the subject to this level (' . implode(' | ', self::LEVELS) . '); omit to show the current level');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $type = Input::string($input, 'type');
        if (! in_array($type, $this->routing->names(), true)) {
            $output->writeln('<error>Unknown event type. Use one of: ' . implode(' | ', $this->routing->names()) . '</error>');

            return Command::INVALID;
        }

        $subject = $this->routing->for($type)->subject;
        $level = Input::stringOrNull($input, 'level');

        if (null === $level) {
            $output->writeln(sprintf('subject <info>%s</info>: compatibility <info>%s</info>', $subject, $this->registry->compatibility($subject)));

            return Command::SUCCESS;
        }

        $level = strtoupper($level);
        if (! in_array($level, self::LEVELS, true)) {
            $output->writeln('<error>Unknown level. Use one of: ' . implode(' | ', self::LEVELS) . '</error>');

            return Command::INVALID;
        }

        try {
            $confirmed = $this->registry->setCompatibility($subject, $level);
        } catch (SchemaRegistrationException $e) {
            $output->writeln('  <error>✗ ' . $e->getMessage() . '</error>');

            return Command::FAILURE;
        }

        $output->writeln(sprintf('subject <info>%s</info>: compatibility set to <info>%s</info>', $subject, $confirmed));

        return Command::SUCCESS;
    }
}
