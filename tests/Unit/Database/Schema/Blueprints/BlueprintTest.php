<?php

declare(strict_types=1);

use Flarme\PhpClickhouse\Database\Schema\Blueprints\DatabaseBlueprint;
use Flarme\PhpClickhouse\Database\Schema\Contracts\BlueprintInterface;

// Note: Blueprint is abstract, so we test it through DatabaseBlueprint which is the simplest concrete implementation

describe('Blueprint (via DatabaseBlueprint)', function (): void {
    describe('construction', function (): void {
        it('implements BlueprintInterface', function (): void {
            $blueprint = new DatabaseBlueprint('test_db');

            expect($blueprint)->toBeInstanceOf(BlueprintInterface::class);
        });

        it('stores the name', function (): void {
            $blueprint = new DatabaseBlueprint('my_database');

            expect($blueprint->getName())->toBe('my_database');
        });

        it('initializes with default values', function (): void {
            $blueprint = new DatabaseBlueprint('test');

            expect($blueprint->shouldUseIfNotExists())->toBeFalse()
                ->and($blueprint->getOnCluster())->toBeNull()
                ->and($blueprint->getComment())->toBeNull();
        });
    });

    describe('ifNotExists', function (): void {
        it('sets ifNotExists flag', function (): void {
            $blueprint = new DatabaseBlueprint('test');

            $result = $blueprint->ifNotExists();

            expect($result)->toBe($blueprint)
                ->and($blueprint->shouldUseIfNotExists())->toBeTrue();
        });

        it('can disable ifNotExists', function (): void {
            $blueprint = new DatabaseBlueprint('test');

            $blueprint->ifNotExists()->ifNotExists(false);

            expect($blueprint->shouldUseIfNotExists())->toBeFalse();
        });
    });

    describe('onCluster', function (): void {
        it('sets cluster name', function (): void {
            $blueprint = new DatabaseBlueprint('test');

            $result = $blueprint->onCluster('my_cluster');

            expect($result)->toBe($blueprint)
                ->and($blueprint->getOnCluster())->toBe('my_cluster');
        });
    });

    describe('comment', function (): void {
        it('sets comment', function (): void {
            $blueprint = new DatabaseBlueprint('test');

            $result = $blueprint->comment('Test database');

            expect($result)->toBe($blueprint)
                ->and($blueprint->getComment())->toBe('Test database');
        });

        it('handles special characters', function (): void {
            $blueprint = new DatabaseBlueprint('test');

            $blueprint->comment("Database for user's data");

            expect($blueprint->getComment())->toBe("Database for user's data");
        });
    });

    describe('fluent interface', function (): void {
        it('supports method chaining', function (): void {
            $blueprint = new DatabaseBlueprint('test');

            $result = $blueprint
                ->ifNotExists()
                ->onCluster('cluster1')
                ->comment('My database');

            expect($result)->toBe($blueprint)
                ->and($blueprint->shouldUseIfNotExists())->toBeTrue()
                ->and($blueprint->getOnCluster())->toBe('cluster1')
                ->and($blueprint->getComment())->toBe('My database');
        });
    });
});
