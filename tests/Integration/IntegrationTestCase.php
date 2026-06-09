<?php

declare(strict_types=1);

namespace Workshop\Tests\Integration;

use Doctrine\DBAL\Connection;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Application;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Tester\CommandTester;
use Symfony\Component\Dotenv\Dotenv;
use Symfony\Component\Uid\Uuid;
use Workshop\Framework\Db\ConnectionFactory;
use Workshop\Framework\Db\SchemaInstaller;
use Workshop\Framework\Kernel;
use Workshop\Tests\Integration\Support\OffsetProbe;

/**
 * Base class for the integration suite: boots the real Kernel once, runs the real
 * commands in-process via CommandTester against the live compose stack (kafka +
 * schema-registry + mysql), and gives every test a clean projection store.
 *
 * Guarded by KAFKA_INTEGRATION=1 (set by `make integration-test`), so a plain
 * `phpunit` run skips the suite instead of hanging on missing infrastructure.
 *
 * Isolation model: topics are shared fixtures whose logs accumulate within a suite
 * run (`make integration-reset` empties them up front), so tests never assert
 * absolute topic state — they use fresh consumer groups, watermark-offset deltas
 * (OffsetProbe), per-test-truncated tables, and the run's own random order ids.
 */
abstract class IntegrationTestCase extends TestCase
{
    private static ?Application $application = null;

    private static ?Connection $connection = null;

    private static ?OffsetProbe $probe = null;

    private static ?string $brokers = null;

    private static ?string $databaseUrl = null;

    public static function setUpBeforeClass(): void
    {
        if ('1' !== getenv('KAFKA_INTEGRATION')) {
            self::markTestSkipped('integration tests run only against the live stack — use `make integration` (sets KAFKA_INTEGRATION=1)');
        }
    }

    protected function setUp(): void
    {
        // Ensure-then-truncate: the tables exist whatever ran before (CREATE TABLE
        // IF NOT EXISTS is cheap) and every test starts from an empty projection.
        new SchemaInstaller($this->db())->install();
        $this->db()->executeStatement('TRUNCATE TABLE orders');
        $this->db()->executeStatement('TRUNCATE TABLE processed_events');
    }

    /**
     * Run a registered console command in-process and return its tester (exit code
     * and display included). Asserts nothing itself.
     *
     * @param array<string, mixed> $input
     * @param array<string, mixed> $options CommandTester execute options, e.g. verbosity
     */
    protected function runCommand(string $name, array $input, array $options = []): CommandTester
    {
        $tester = new CommandTester(self::application()->find($name));
        $tester->execute($input, $options);

        return $tester;
    }

    /**
     * Run bin/console as a real child process — the end-to-end CLI surface (binary,
     * shortcuts, exit code), which in-process CommandTester runs bypass.
     *
     * @return array{exit: int, output: string}
     */
    protected static function console(string $argLine): array
    {
        exec(sprintf('php %s %s 2>&1', escapeshellarg(self::projectDir() . '/bin/console'), $argLine), $lines, $exit);

        return [
            'exit' => $exit,
            'output' => implode("\n", $lines),
        ];
    }

    /**
     * Seed messages through the real produce command and assert the run succeeded.
     *
     * @param array<string, mixed> $options extra kafka:produce:sample input, e.g. ['--pool' => '1']
     */
    protected function produce(int $count, ?string $messageName = null, array $options = []): CommandTester
    {
        $input = $options + [
            '--count' => (string) $count,
            '--interval' => '0',
        ];
        if (null !== $messageName) {
            $input['--message-name'] = $messageName;
        }

        $tester = $this->runCommand('kafka:produce:sample', $input);
        self::assertSame(Command::SUCCESS, $tester->getStatusCode(), $tester->getDisplay());

        return $tester;
    }

    /**
     * Consume the topic's entire current backlog deterministically: --max is set to
     * the broker's end-offset total (so the run can neither under-read during the
     * group join nor over-read messages produced later) and --ttl is a hard safety
     * ceiling so a wedged consumer can never hang the suite.
     *
     * @param array<string, mixed> $input extra kafka:consume input, e.g. ['--profile' => 'default']
     */
    protected function consumeBacklog(string $topic, array $input = [], int $ttlMs = 60000): CommandTester
    {
        $backlog = $this->probe()->totalEnd($topic);

        $tester = $this->runCommand('kafka:consume', $input + [
            'topic' => $topic,
            '--from' => 'beginning',
            '--max' => (string) $backlog,
            '--ttl' => (string) $ttlMs,
        ]);
        self::assertSame(Command::SUCCESS, $tester->getStatusCode(), $tester->getDisplay());

        return $tester;
    }

    /**
     * The partition keys (order ids) a produce run reported, in send order.
     *
     * @return list<string>
     */
    protected static function producedKeys(CommandTester $producer): array
    {
        preg_match_all('/key=(\S+)/', $producer->getDisplay(), $matches);

        return array_values($matches[1]);
    }

    protected function db(): Connection
    {
        if (null === self::$connection) {
            self::$connection = new ConnectionFactory(self::databaseUrl())->create();
        }

        return self::$connection;
    }

    protected function probe(): OffsetProbe
    {
        return self::$probe ??= new OffsetProbe(self::brokers());
    }

    /**
     * A fresh, suite-unique consumer group id, so committed-offset state never
     * leaks between tests (or into the workshop's own demo groups).
     */
    protected function uniqueGroup(): string
    {
        return 'it-' . Uuid::v4()->toRfc4122();
    }

    protected static function brokers(): string
    {
        self::application();
        \assert(null !== self::$brokers);

        return self::$brokers;
    }

    protected static function projectDir(): string
    {
        return \dirname(__DIR__, 2);
    }

    private static function databaseUrl(): string
    {
        self::application();
        \assert(null !== self::$databaseUrl);

        return self::$databaseUrl;
    }

    /**
     * The real application, booted once per phpunit process exactly like
     * bin/console: dotenv, Kernel, every console.command-tagged service.
     */
    private static function application(): Application
    {
        if (null !== self::$application) {
            return self::$application;
        }

        new Dotenv()->bootEnv(self::projectDir() . '/.env');

        $container = new Kernel(self::projectDir())->boot();

        $brokers = $container->getParameter('kafka.brokers');
        self::$brokers = \is_string($brokers) ? $brokers : '';
        $databaseUrl = $container->getParameter('database.url');
        self::$databaseUrl = \is_string($databaseUrl) ? $databaseUrl : '';

        $application = new Application('kafka-php-workshop-integration', 'test');
        $application->setAutoExit(false);
        foreach (array_keys($container->findTaggedServiceIds('console.command')) as $id) {
            $command = $container->get($id);
            if ($command instanceof Command) {
                $application->addCommand($command);
            }
        }

        return self::$application = $application;
    }
}
