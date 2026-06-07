<?php

declare(strict_types=1);

namespace Workshop\Kafka\Config;

use RdKafka\Conf;

/**
 * The single seam between config-as-data and the extension: the only place in the
 * workshop that turns a KafkaProfile (plus runtime parameters like group.id) into
 * a live \RdKafka\Conf. Because one method owns every $conf->set() call, "trace a
 * setting from the table to the running client" has exactly one answer.
 *
 * Precedence, last write wins: broker list + client.id defaults → profile settings
 * → runtime overrides. The broker is probed first so a dead stack fails fast.
 */
final readonly class ConfBuilder
{
    public function __construct(
        private string $brokers,
        private BrokerProbe $probe,
    ) {
    }

    /**
     * @param array<string, string|int> $runtime librdkafka overrides applied last (e.g. group.id)
     */
    public function build(KafkaProfile $profile, array $runtime = []): Conf
    {
        $this->probe->assertReachable($this->brokers);

        $conf = new Conf();
        $conf->set('metadata.broker.list', $this->brokers);
        // A readable client.id so every workshop client is identifiable broker-side.
        $conf->set('client.id', sprintf('workshop.%s.%d', $profile->role->value, getmypid()));

        foreach ($profile->settings as $setting) {
            $value = $this->resolve($setting->value);
            if (null === $value) {
                continue; // unresolved placeholder — not literal librdkafka input
            }
            $conf->set($setting->key, $value);
        }

        foreach ($runtime as $key => $value) {
            $conf->set($key, (string) $value);
        }

        return $conf;
    }

    /**
     * Resolve the one dynamic placeholder the config model uses (group.instance.id =
     * gethostname()); skip any other value that looks like an unevaluated call.
     */
    private function resolve(string $value): ?string
    {
        if ('gethostname()' === $value) {
            return gethostname() ?: 'php-client';
        }

        if (str_ends_with($value, ')')) {
            return null;
        }

        return $value;
    }
}
