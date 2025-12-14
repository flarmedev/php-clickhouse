<?php

declare(strict_types=1);

use Flarme\PhpClickhouse\Database\Schema\Builder;
use Flarme\PhpClickhouse\Database\Schema\Grammar;

describe('Schema Builder', function (): void {
    describe('construction', function (): void {
        it('can be instantiated without client', function (): void {
            $builder = new Builder();

            expect($builder)->toBeInstanceOf(Builder::class);
        });

        it('initializes grammar', function (): void {
            $builder = new Builder();

            expect($builder->getGrammar())->toBeInstanceOf(Grammar::class);
        });
    });

    describe('static connection', function (): void {
        it('creates new builder instance', function (): void {
            $builder = Builder::connection();

            expect($builder)->toBeInstanceOf(Builder::class);
        });
    });

    describe('onCluster', function (): void {
        it('sets default cluster', function (): void {
            $builder = new Builder();

            $result = $builder->onCluster('my_cluster');

            expect($result)->toBe($builder);
        });

        it('can clear cluster', function (): void {
            $builder = new Builder();

            $builder->onCluster('my_cluster')->onCluster(null);

            // No exception means it worked
            expect(true)->toBeTrue();
        });
    });

    describe('createDatabase', function (): void {
        it('returns SQL string', function (): void {
            $builder = new Builder();

            $sql = $builder->createDatabase('test_db');

            expect($sql)->toBe('CREATE DATABASE `test_db`');
        });

        it('accepts callback for configuration', function (): void {
            $builder = new Builder();

            $sql = $builder->createDatabase('test_db', function ($blueprint): void {
                $blueprint->ifNotExists();
                $blueprint->atomic();
            });

            expect($sql)->toContain('IF NOT EXISTS')
                ->and($sql)->toContain('ENGINE = Atomic');
        });

        it('uses default cluster', function (): void {
            $builder = new Builder();
            $builder->onCluster('my_cluster');

            $sql = $builder->createDatabase('test_db');

            expect($sql)->toContain('ON CLUSTER `my_cluster`');
        });
    });

    describe('dropDatabase', function (): void {
        it('returns SQL string', function (): void {
            $builder = new Builder();

            $sql = $builder->dropDatabase('test_db');

            expect($sql)->toBe('DROP DATABASE `test_db`');
        });

        it('uses default cluster', function (): void {
            $builder = new Builder();
            $builder->onCluster('my_cluster');

            $sql = $builder->dropDatabase('test_db');

            expect($sql)->toContain('ON CLUSTER `my_cluster`');
        });
    });

    describe('dropDatabaseIfExists', function (): void {
        it('returns SQL string with IF EXISTS', function (): void {
            $builder = new Builder();

            $sql = $builder->dropDatabaseIfExists('test_db');

            expect($sql)->toBe('DROP DATABASE IF EXISTS `test_db`');
        });
    });

    describe('create', function (): void {
        it('returns SQL string', function (): void {
            $builder = new Builder();

            $sql = $builder->create('users', function ($table): void {
                $table->uint64('id');
                $table->string('name');
                $table->mergeTree();
                $table->orderBy('id');
            });

            expect($sql)->toContain('CREATE TABLE `users`')
                ->and($sql)->toContain('`id` UInt64')
                ->and($sql)->toContain('`name` String')
                ->and($sql)->toContain('ENGINE = MergeTree');
        });

        it('uses default cluster', function (): void {
            $builder = new Builder();
            $builder->onCluster('my_cluster');

            $sql = $builder->create('users', function ($table): void {
                $table->uint64('id');
                $table->mergeTree();
                $table->orderBy('id');
            });

            expect($sql)->toContain('ON CLUSTER `my_cluster`');
        });
    });

    describe('alter', function (): void {
        it('returns array of SQL strings', function (): void {
            $builder = new Builder();

            $statements = $builder->alter('users', function ($table): void {
                $table->addColumn('email', 'String');
                $table->dropColumn('old_column');
            });

            expect($statements)->toBeArray()
                ->and($statements)->toHaveCount(2)
                ->and($statements[0])->toContain('ADD COLUMN')
                ->and($statements[1])->toContain('DROP COLUMN');
        });

        it('uses default cluster', function (): void {
            $builder = new Builder();
            $builder->onCluster('my_cluster');

            $statements = $builder->alter('users', function ($table): void {
                $table->addColumn('email', 'String');
            });

            expect($statements[0])->toContain('ON CLUSTER `my_cluster`');
        });
    });

    describe('drop', function (): void {
        it('returns SQL string', function (): void {
            $builder = new Builder();

            $sql = $builder->drop('users');

            expect($sql)->toBe('DROP TABLE `users`');
        });
    });

    describe('dropIfExists', function (): void {
        it('returns SQL string with IF EXISTS', function (): void {
            $builder = new Builder();

            $sql = $builder->dropIfExists('users');

            expect($sql)->toBe('DROP TABLE IF EXISTS `users`');
        });
    });

    describe('rename', function (): void {
        it('returns SQL string', function (): void {
            $builder = new Builder();

            $sql = $builder->rename('old_table', 'new_table');

            expect($sql)->toBe('RENAME TABLE `old_table` TO `new_table`');
        });
    });

    describe('truncate', function (): void {
        it('returns SQL string', function (): void {
            $builder = new Builder();

            $sql = $builder->truncate('users');

            expect($sql)->toBe('TRUNCATE TABLE `users`');
        });
    });

    describe('createView', function (): void {
        it('returns SQL string', function (): void {
            $builder = new Builder();

            $sql = $builder->createView('my_view', function ($view): void {
                $view->as('SELECT * FROM users');
            });

            expect($sql)->toContain('CREATE VIEW `my_view`')
                ->and($sql)->toContain('AS SELECT * FROM users');
        });
    });

    describe('createMaterializedView', function (): void {
        it('returns SQL string', function (): void {
            $builder = new Builder();

            $sql = $builder->createMaterializedView('my_mv', function ($view): void {
                $view->to('target_table');
                $view->as('SELECT * FROM source');
            });

            expect($sql)->toContain('CREATE MATERIALIZED VIEW `my_mv`')
                ->and($sql)->toContain('TO `target_table`');
        });
    });

    describe('dropView', function (): void {
        it('returns SQL string', function (): void {
            $builder = new Builder();

            $sql = $builder->dropView('my_view');

            expect($sql)->toBe('DROP VIEW `my_view`');
        });
    });

    describe('dropViewIfExists', function (): void {
        it('returns SQL string with IF EXISTS', function (): void {
            $builder = new Builder();

            $sql = $builder->dropViewIfExists('my_view');

            expect($sql)->toBe('DROP VIEW IF EXISTS `my_view`');
        });
    });

    describe('createDictionary', function (): void {
        it('returns SQL string', function (): void {
            $builder = new Builder();

            $sql = $builder->createDictionary('my_dict', function ($dict): void {
                $dict->primaryKey('id');
                $dict->attribute('name', 'String', '');
                $dict->sourceClickHouse('source_table');
                $dict->layoutFlat();
                $dict->lifetime(300);
            });

            expect($sql)->toContain('CREATE DICTIONARY `my_dict`')
                ->and($sql)->toContain('PRIMARY KEY `id`');
        });
    });

    describe('dropDictionary', function (): void {
        it('returns SQL string', function (): void {
            $builder = new Builder();

            $sql = $builder->dropDictionary('my_dict');

            expect($sql)->toBe('DROP DICTIONARY `my_dict`');
        });
    });

    describe('dropDictionaryIfExists', function (): void {
        it('returns SQL string with IF EXISTS', function (): void {
            $builder = new Builder();

            $sql = $builder->dropDictionaryIfExists('my_dict');

            expect($sql)->toBe('DROP DICTIONARY IF EXISTS `my_dict`');
        });
    });

    describe('utility methods without client', function (): void {
        describe('hasTable', function (): void {
            it('returns false without client', function (): void {
                $builder = new Builder();

                expect($builder->hasTable('users'))->toBeFalse();
            });
        });

        describe('hasColumn', function (): void {
            it('returns false without client', function (): void {
                $builder = new Builder();

                expect($builder->hasColumn('users', 'id'))->toBeFalse();
            });
        });

        describe('hasDatabase', function (): void {
            it('returns false without client', function (): void {
                $builder = new Builder();

                expect($builder->hasDatabase('test_db'))->toBeFalse();
            });
        });

        describe('hasView', function (): void {
            it('returns false without client', function (): void {
                $builder = new Builder();

                expect($builder->hasView('my_view'))->toBeFalse();
            });
        });

        describe('hasDictionary', function (): void {
            it('returns false without client', function (): void {
                $builder = new Builder();

                expect($builder->hasDictionary('my_dict'))->toBeFalse();
            });
        });

        describe('getColumns', function (): void {
            it('returns empty array without client', function (): void {
                $builder = new Builder();

                expect($builder->getColumns('users'))->toBe([]);
            });
        });
    });

    describe('getGrammar', function (): void {
        it('returns grammar instance', function (): void {
            $builder = new Builder();

            expect($builder->getGrammar())->toBeInstanceOf(Grammar::class);
        });
    });
});

/**
 * CODE REVIEW NOTES for Schema Builder.php:
 *
 * 1. GOOD: Clean API for schema operations
 * 2. GOOD: Support for default ON CLUSTER clause
 * 3. GOOD: Utility methods for checking existence (hasTable, hasColumn, etc.)
 * 4. GOOD: Returns SQL strings for inspection/debugging
 *
 * POTENTIAL IMPROVEMENTS:
 * 1. Consider adding transaction support for multiple operations
 * 2. Could add dry-run mode that only returns SQL without executing
 * 3. The execute method is protected - consider making it configurable
 * 4. Could add batch operations for multiple tables
 */
