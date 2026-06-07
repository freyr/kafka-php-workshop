<?php

declare(strict_types=1);

use PhpCsFixer\Fixer\Phpdoc\PhpdocToCommentFixer;
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
    )
    // Keep inline `/** @var ... */` hints as real docblocks. The @Symfony set's
    // PhpdocToCommentFixer otherwise demotes any /** */ not attached to a
    // structural element to /* */, which PHPStan then ignores.
    ->withConfiguredRule(PhpdocToCommentFixer::class, ['ignored_tags' => ['var']]);
