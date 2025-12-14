<?php

declare(strict_types=1);

use Flarme\PhpClickhouse\Database\Schema\Blueprints\DatabaseBlueprint;

describe('DatabaseBlueprint', function (): void {
    describe('construction', function (): void {
        it('initializes with null engine', function (): void {
            $blueprint = new DatabaseBlueprint('test_db');

            expect($blueprint->getEngine())->toBeNull();
        });
    });

    describe('engine', function (): void {
        it('sets custom engine', function (): void {
            $blueprint = new DatabaseBlueprint('test_db');

            $result = $blueprint->engine('Atomic');

            expect($result)->toBe($blueprint)
                ->and($blueprint->getEngine())->toBe('Atomic');
        });
    });

    describe('atomic', function (): void {
        it('sets Atomic engine', function (): void {
            $blueprint = new DatabaseBlueprint('test_db');

            $result = $blueprint->atomic();

            expect($result)->toBe($blueprint)
                ->and($blueprint->getEngine())->toBe('Atomic');
        });
    });

    describe('ordinary', function (): void {
        it('sets Ordinary engine', function (): void {
            $blueprint = new DatabaseBlueprint('test_db');

            $result = $blueprint->ordinary();

            expect($result)->toBe($blueprint)
                ->and($blueprint->getEngine())->toBe('Ordinary');
        });
    });

    describe('lazy', function (): void {
        it('sets Lazy engine with default expiration', function (): void {
            $blueprint = new DatabaseBlueprint('test_db');

            $result = $blueprint->lazy();

            expect($result)->toBe($blueprint)
                ->and($blueprint->getEngine())->toBe('Lazy(3600)');
        });

        it('sets Lazy engine with custom expiration', function (): void {
            $blueprint = new DatabaseBlueprint('test_db');

            $blueprint->lazy(7200);

            expect($blueprint->getEngine())->toBe('Lazy(7200)');
        });
    });

    describe('replicated', function (): void {
        it('sets Replicated engine', function (): void {
            $blueprint = new DatabaseBlueprint('test_db');

            $result = $blueprint->replicated('/clickhouse/databases/test', 'shard1', 'replica1');

            expect($result)->toBe($blueprint)
                ->and($blueprint->getEngine())->toBe("Replicated('/clickhouse/databases/test', 'shard1', 'replica1')");
        });
    });

    describe('fluent interface', function (): void {
        it('supports method chaining with engine', function (): void {
            $blueprint = new DatabaseBlueprint('test_db');

            $result = $blueprint
                ->ifNotExists()
                ->onCluster('my_cluster')
                ->atomic()
                ->comment('Test database');

            expect($result)->toBe($blueprint)
                ->and($blueprint->shouldUseIfNotExists())->toBeTrue()
                ->and($blueprint->getOnCluster())->toBe('my_cluster')
                ->and($blueprint->getEngine())->toBe('Atomic')
                ->and($blueprint->getComment())->toBe('Test database');
        });
    });
});

/**
 * CODE REVIEW NOTES for DatabaseBlueprint.php:
 *
 * 1. GOOD: Simple and focused - only handles database-specific options
 * 2. GOOD: Convenience methods for common engines (atomic, ordinary, lazy, replicated)
 * 3. GOOD: Lazy engine properly formats the expiration time
 *
 * POTENTIAL IMPROVEMENTS:
 * 1. Consider adding validation for engine names
 * 2. The replicated() method builds the engine string inline - could be more consistent with other methods
 */
