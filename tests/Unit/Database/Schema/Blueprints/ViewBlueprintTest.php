<?php

declare(strict_types=1);

use Flarme\PhpClickhouse\Database\Schema\Blueprints\ViewBlueprint;
use Flarme\PhpClickhouse\Database\Schema\Components\Column;

describe('ViewBlueprint', function (): void {
    describe('construction', function (): void {
        it('initializes with default values', function (): void {
            $blueprint = new ViewBlueprint('my_view');

            expect($blueprint->getName())->toBe('my_view')
                ->and($blueprint->isMaterialized())->toBeFalse()
                ->and($blueprint->getToTable())->toBeNull()
                ->and($blueprint->getToDatabase())->toBeNull()
                ->and($blueprint->getEngine())->toBeNull()
                ->and($blueprint->getEngineParams())->toBe([])
                ->and($blueprint->getPartitionBy())->toBeNull()
                ->and($blueprint->getOrderBy())->toBe([])
                ->and($blueprint->getPrimaryKey())->toBe([])
                ->and($blueprint->getTtl())->toBeNull()
                ->and($blueprint->getSettings())->toBe([])
                ->and($blueprint->getAsSelect())->toBeNull()
                ->and($blueprint->shouldPopulate())->toBeFalse()
                ->and($blueprint->isRefreshable())->toBeFalse()
                ->and($blueprint->getRefreshInterval())->toBeNull()
                ->and($blueprint->getColumns())->toBe([]);
        });
    });

    describe('materialized', function (): void {
        it('sets materialized flag', function (): void {
            $blueprint = new ViewBlueprint('my_view');

            $result = $blueprint->materialized();

            expect($result)->toBe($blueprint)
                ->and($blueprint->isMaterialized())->toBeTrue();
        });

        it('can disable materialized', function (): void {
            $blueprint = new ViewBlueprint('my_view');

            $blueprint->materialized()->materialized(false);

            expect($blueprint->isMaterialized())->toBeFalse();
        });
    });

    describe('to', function (): void {
        it('sets target table', function (): void {
            $blueprint = new ViewBlueprint('my_view');

            $result = $blueprint->to('target_table');

            expect($result)->toBe($blueprint)
                ->and($blueprint->getToTable())->toBe('target_table')
                ->and($blueprint->getToDatabase())->toBeNull();
        });

        it('sets target table with database', function (): void {
            $blueprint = new ViewBlueprint('my_view');

            $blueprint->to('target_table', 'other_db');

            expect($blueprint->getToTable())->toBe('target_table')
                ->and($blueprint->getToDatabase())->toBe('other_db');
        });
    });

    describe('as', function (): void {
        it('sets SELECT query', function (): void {
            $blueprint = new ViewBlueprint('my_view');

            $result = $blueprint->as('SELECT * FROM users');

            expect($result)->toBe($blueprint)
                ->and($blueprint->getAsSelect())->toBe('SELECT * FROM users');
        });
    });

    describe('populate', function (): void {
        it('sets populate flag', function (): void {
            $blueprint = new ViewBlueprint('my_view');

            $result = $blueprint->populate();

            expect($result)->toBe($blueprint)
                ->and($blueprint->shouldPopulate())->toBeTrue();
        });

        it('can disable populate', function (): void {
            $blueprint = new ViewBlueprint('my_view');

            $blueprint->populate()->populate(false);

            expect($blueprint->shouldPopulate())->toBeFalse();
        });
    });

    describe('refreshable', function (): void {
        it('sets refreshable with interval', function (): void {
            $blueprint = new ViewBlueprint('my_view');

            $result = $blueprint->refreshable('EVERY 1 HOUR');

            expect($result)->toBe($blueprint)
                ->and($blueprint->isRefreshable())->toBeTrue()
                ->and($blueprint->getRefreshInterval())->toBe('EVERY 1 HOUR');
        });
    });

    describe('engine configuration', function (): void {
        describe('engine', function (): void {
            it('sets custom engine', function (): void {
                $blueprint = new ViewBlueprint('my_view');

                $result = $blueprint->engine('MergeTree', 'param1');

                expect($result)->toBe($blueprint)
                    ->and($blueprint->getEngine())->toBe('MergeTree')
                    ->and($blueprint->getEngineParams())->toBe(['param1']);
            });
        });

        describe('mergeTree', function (): void {
            it('sets MergeTree engine', function (): void {
                $blueprint = new ViewBlueprint('my_view');

                expect($blueprint->mergeTree()->getEngine())->toBe('MergeTree');
            });
        });

        describe('replacingMergeTree', function (): void {
            it('sets ReplacingMergeTree engine', function (): void {
                $blueprint = new ViewBlueprint('my_view');

                expect($blueprint->replacingMergeTree()->getEngine())->toBe('ReplacingMergeTree');
            });

            it('sets ReplacingMergeTree with version column', function (): void {
                $blueprint = new ViewBlueprint('my_view');

                $blueprint->replacingMergeTree('version');

                expect($blueprint->getEngine())->toBe('ReplacingMergeTree')
                    ->and($blueprint->getEngineParams())->toBe(['version']);
            });
        });

        describe('summingMergeTree', function (): void {
            it('sets SummingMergeTree engine', function (): void {
                $blueprint = new ViewBlueprint('my_view');

                expect($blueprint->summingMergeTree()->getEngine())->toBe('SummingMergeTree');
            });

            it('sets SummingMergeTree with columns', function (): void {
                $blueprint = new ViewBlueprint('my_view');

                $blueprint->summingMergeTree(['amount']);

                expect($blueprint->getEngineParams())->toBe(['amount']);
            });
        });

        describe('aggregatingMergeTree', function (): void {
            it('sets AggregatingMergeTree engine', function (): void {
                $blueprint = new ViewBlueprint('my_view');

                expect($blueprint->aggregatingMergeTree()->getEngine())->toBe('AggregatingMergeTree');
            });
        });
    });

    describe('table structure', function (): void {
        describe('partitionBy', function (): void {
            it('sets partition expression', function (): void {
                $blueprint = new ViewBlueprint('my_view');

                $result = $blueprint->partitionBy('toYYYYMM(date)');

                expect($result)->toBe($blueprint)
                    ->and($blueprint->getPartitionBy())->toBe('toYYYYMM(date)');
            });
        });

        describe('orderBy', function (): void {
            it('sets order by from string', function (): void {
                $blueprint = new ViewBlueprint('my_view');

                $result = $blueprint->orderBy('id');

                expect($result)->toBe($blueprint)
                    ->and($blueprint->getOrderBy())->toBe(['id']);
            });

            it('sets order by from array', function (): void {
                $blueprint = new ViewBlueprint('my_view');

                $blueprint->orderBy(['id', 'date']);

                expect($blueprint->getOrderBy())->toBe(['id', 'date']);
            });
        });

        describe('primaryKey', function (): void {
            it('sets primary key from string', function (): void {
                $blueprint = new ViewBlueprint('my_view');

                $result = $blueprint->primaryKey('id');

                expect($result)->toBe($blueprint)
                    ->and($blueprint->getPrimaryKey())->toBe(['id']);
            });

            it('sets primary key from array', function (): void {
                $blueprint = new ViewBlueprint('my_view');

                $blueprint->primaryKey(['id', 'tenant_id']);

                expect($blueprint->getPrimaryKey())->toBe(['id', 'tenant_id']);
            });
        });

        describe('ttl', function (): void {
            it('sets TTL expression', function (): void {
                $blueprint = new ViewBlueprint('my_view');

                $result = $blueprint->ttl('date + INTERVAL 1 MONTH');

                expect($result)->toBe($blueprint)
                    ->and($blueprint->getTtl())->toBe('date + INTERVAL 1 MONTH');
            });
        });

        describe('settings', function (): void {
            it('sets table settings', function (): void {
                $blueprint = new ViewBlueprint('my_view');

                $result = $blueprint->settings(['index_granularity' => 8192]);

                expect($result)->toBe($blueprint)
                    ->and($blueprint->getSettings())->toBe(['index_granularity' => 8192]);
            });

            it('merges settings', function (): void {
                $blueprint = new ViewBlueprint('my_view');

                $blueprint->settings(['a' => 1])->settings(['b' => 2]);

                expect($blueprint->getSettings())->toBe(['a' => 1, 'b' => 2]);
            });
        });
    });

    describe('column definitions', function (): void {
        describe('column', function (): void {
            it('adds column definition', function (): void {
                $blueprint = new ViewBlueprint('my_view');

                $column = $blueprint->column('id', 'UInt64');

                expect($column)->toBeInstanceOf(Column::class)
                    ->and($column->getName())->toBe('id')
                    ->and($column->getType())->toBe('UInt64')
                    ->and($blueprint->getColumns())->toHaveCount(1);
            });

            it('adds multiple columns', function (): void {
                $blueprint = new ViewBlueprint('my_view');

                $blueprint->column('id', 'UInt64');
                $blueprint->column('name', 'String');

                expect($blueprint->getColumns())->toHaveCount(2);
            });
        });
    });

    describe('fluent interface', function (): void {
        it('supports method chaining for regular view', function (): void {
            $blueprint = new ViewBlueprint('my_view');

            $result = $blueprint
                ->ifNotExists()
                ->as('SELECT * FROM users')
                ->comment('User view');

            expect($result)->toBe($blueprint)
                ->and($blueprint->shouldUseIfNotExists())->toBeTrue()
                ->and($blueprint->getAsSelect())->toBe('SELECT * FROM users')
                ->and($blueprint->getComment())->toBe('User view');
        });

        it('supports method chaining for materialized view', function (): void {
            $blueprint = new ViewBlueprint('my_mv');

            $result = $blueprint
                ->materialized()
                ->to('target_table')
                ->as('SELECT * FROM source')
                ->populate();

            expect($result)->toBe($blueprint)
                ->and($blueprint->isMaterialized())->toBeTrue()
                ->and($blueprint->getToTable())->toBe('target_table')
                ->and($blueprint->getAsSelect())->toBe('SELECT * FROM source')
                ->and($blueprint->shouldPopulate())->toBeTrue();
        });

        it('supports method chaining for materialized view with storage', function (): void {
            $blueprint = new ViewBlueprint('my_mv');

            $result = $blueprint
                ->materialized()
                ->mergeTree()
                ->orderBy('id')
                ->partitionBy('toYYYYMM(date)')
                ->as('SELECT * FROM source');

            expect($result)->toBe($blueprint)
                ->and($blueprint->isMaterialized())->toBeTrue()
                ->and($blueprint->getEngine())->toBe('MergeTree')
                ->and($blueprint->getOrderBy())->toBe(['id'])
                ->and($blueprint->getPartitionBy())->toBe('toYYYYMM(date)');
        });
    });
});

/**
 * CODE REVIEW NOTES for ViewBlueprint.php:
 *
 * 1. GOOD: Clean separation between regular views and materialized views
 * 2. GOOD: Support for TO clause and inline storage definition
 * 3. GOOD: Support for refreshable materialized views
 * 4. GOOD: Optional column definitions for explicit schema
 *
 * POTENTIAL IMPROVEMENTS:
 * 1. Consider adding validation that TO clause and inline storage are mutually exclusive
 * 2. Could add more engine types specific to materialized views
 * 3. The refreshable interval could have validation for valid ClickHouse syntax
 */
