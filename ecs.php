<?php

declare(strict_types=1);

use Symplify\EasyCodingStandard\Config\ECSConfig;

/** @noinspection PhpUnhandledExceptionInspection */
return ECSConfig::configure()
    ->withRootFiles()
    ->withPaths([
        __DIR__ . '/src',
        __DIR__ . '/tests',
        __DIR__ . '/config',
        __DIR__ . '/bin/console',
    ])->withPhpCsFixerSets(
        perCS: true,
        symfony: true,
    )->withPreparedSets(
        arrays: true,
        comments: true,
        docblocks: true,
        spaces: true,
        namespaces: true,
    );
