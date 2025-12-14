<?php

declare(strict_types=1);

use Flarme\PhpClickhouse\Database\Schema\Blueprints\DictionaryBlueprint;
use Flarme\PhpClickhouse\Database\Schema\Components\Attribute;

describe('DictionaryBlueprint', function (): void {
    describe('construction', function (): void {
        it('initializes with default values', function (): void {
            $blueprint = new DictionaryBlueprint('my_dict');

            expect($blueprint->getName())->toBe('my_dict')
                ->and($blueprint->getPrimaryKey())->toBe([])
                ->and($blueprint->getAttributes())->toBe([])
                ->and($blueprint->getSource())->toBeNull()
                ->and($blueprint->getLayout())->toBeNull()
                ->and($blueprint->getLifetime())->toBeNull()
                ->and($blueprint->getRange())->toBeNull();
        });
    });

    describe('primaryKey', function (): void {
        it('sets primary key from string', function (): void {
            $blueprint = new DictionaryBlueprint('my_dict');

            $result = $blueprint->primaryKey('id');

            expect($result)->toBe($blueprint)
                ->and($blueprint->getPrimaryKey())->toBe(['id']);
        });

        it('sets primary key from array', function (): void {
            $blueprint = new DictionaryBlueprint('my_dict');

            $blueprint->primaryKey(['id', 'tenant_id']);

            expect($blueprint->getPrimaryKey())->toBe(['id', 'tenant_id']);
        });
    });

    describe('attributes', function (): void {
        describe('attribute', function (): void {
            it('adds attribute without default', function (): void {
                $blueprint = new DictionaryBlueprint('my_dict');

                $attribute = $blueprint->attribute('name', 'String');

                expect($attribute)->toBeInstanceOf(Attribute::class)
                    ->and($attribute->getName())->toBe('name')
                    ->and($attribute->getType())->toBe('String')
                    ->and($attribute->hasDefault())->toBeFalse()
                    ->and($blueprint->getAttributes())->toHaveCount(1);
            });

            it('adds attribute with default', function (): void {
                $blueprint = new DictionaryBlueprint('my_dict');

                $attribute = $blueprint->attribute('status', 'String', 'unknown');

                expect($attribute->hasDefault())->toBeTrue()
                    ->and($attribute->getDefault())->toBe('unknown');
            });

            it('adds attribute with null default', function (): void {
                $blueprint = new DictionaryBlueprint('my_dict');

                $attribute = $blueprint->attribute('optional', 'String', null);

                expect($attribute->hasDefault())->toBeTrue()
                    ->and($attribute->getDefault())->toBeNull();
            });
        });

        describe('hierarchicalAttribute', function (): void {
            it('adds hierarchical attribute', function (): void {
                $blueprint = new DictionaryBlueprint('my_dict');

                $attribute = $blueprint->hierarchicalAttribute('parent_id', 'UInt64', 0);

                expect($attribute->isHierarchical())->toBeTrue()
                    ->and($attribute->hasDefault())->toBeTrue()
                    ->and($attribute->getDefault())->toBe(0);
            });
        });

        describe('injectiveAttribute', function (): void {
            it('adds injective attribute', function (): void {
                $blueprint = new DictionaryBlueprint('my_dict');

                $attribute = $blueprint->injectiveAttribute('code', 'String', '');

                expect($attribute->isInjective())->toBeTrue();
            });
        });

        describe('expressionAttribute', function (): void {
            it('adds expression attribute', function (): void {
                $blueprint = new DictionaryBlueprint('my_dict');

                $attribute = $blueprint->expressionAttribute('computed', 'UInt64', 'id * 2');

                expect($attribute->isExpression())->toBeTrue()
                    ->and($attribute->getExpression())->toBe('id * 2');
            });
        });
    });

    describe('sources', function (): void {
        describe('sourceClickHouse', function (): void {
            it('sets ClickHouse source', function (): void {
                $blueprint = new DictionaryBlueprint('my_dict');

                $result = $blueprint->sourceClickHouse('source_table');

                expect($result)->toBe($blueprint)
                    ->and($blueprint->getSource())->toBe([
                        'type' => 'clickhouse',
                        'table' => 'source_table',
                        'database' => null,
                        'options' => [],
                    ]);
            });

            it('sets ClickHouse source with database', function (): void {
                $blueprint = new DictionaryBlueprint('my_dict');

                $blueprint->sourceClickHouse('source_table', 'other_db');

                expect($blueprint->getSource()['database'])->toBe('other_db');
            });

            it('sets ClickHouse source with options', function (): void {
                $blueprint = new DictionaryBlueprint('my_dict');

                $blueprint->sourceClickHouse('source_table', null, ['where' => 'active = 1']);

                expect($blueprint->getSource()['options'])->toBe(['where' => 'active = 1']);
            });
        });

        describe('sourceMySQL', function (): void {
            it('sets MySQL source', function (): void {
                $blueprint = new DictionaryBlueprint('my_dict');

                $result = $blueprint->sourceMySQL('localhost', 3306, 'user', 'pass', 'db', 'table');

                expect($result)->toBe($blueprint)
                    ->and($blueprint->getSource())->toBe([
                        'type' => 'mysql',
                        'host' => 'localhost',
                        'port' => 3306,
                        'user' => 'user',
                        'password' => 'pass',
                        'database' => 'db',
                        'table' => 'table',
                        'options' => [],
                    ]);
            });

            it('sets MySQL source with options', function (): void {
                $blueprint = new DictionaryBlueprint('my_dict');

                $blueprint->sourceMySQL('localhost', 3306, 'user', 'pass', 'db', 'table', ['ssl' => true]);

                expect($blueprint->getSource()['options'])->toBe(['ssl' => true]);
            });
        });

        describe('sourcePostgreSQL', function (): void {
            it('sets PostgreSQL source', function (): void {
                $blueprint = new DictionaryBlueprint('my_dict');

                $result = $blueprint->sourcePostgreSQL('localhost', 5432, 'user', 'pass', 'db', 'table');

                expect($result)->toBe($blueprint)
                    ->and($blueprint->getSource()['type'])->toBe('postgresql');
            });

            it('sets PostgreSQL source with schema', function (): void {
                $blueprint = new DictionaryBlueprint('my_dict');

                $blueprint->sourcePostgreSQL('localhost', 5432, 'user', 'pass', 'db', 'table', 'public');

                expect($blueprint->getSource()['schema'])->toBe('public');
            });
        });

        describe('sourceMongoDB', function (): void {
            it('sets MongoDB source', function (): void {
                $blueprint = new DictionaryBlueprint('my_dict');

                $result = $blueprint->sourceMongoDB('localhost', 27017, 'user', 'pass', 'db', 'collection');

                expect($result)->toBe($blueprint)
                    ->and($blueprint->getSource()['type'])->toBe('mongodb')
                    ->and($blueprint->getSource()['collection'])->toBe('collection');
            });
        });

        describe('sourceHTTP', function (): void {
            it('sets HTTP source', function (): void {
                $blueprint = new DictionaryBlueprint('my_dict');

                $result = $blueprint->sourceHTTP('http://example.com/data', 'JSONEachRow');

                expect($result)->toBe($blueprint)
                    ->and($blueprint->getSource())->toBe([
                        'type' => 'http',
                        'url' => 'http://example.com/data',
                        'format' => 'JSONEachRow',
                        'options' => [],
                    ]);
            });
        });

        describe('sourceFile', function (): void {
            it('sets File source', function (): void {
                $blueprint = new DictionaryBlueprint('my_dict');

                $result = $blueprint->sourceFile('/path/to/file.csv', 'CSV');

                expect($result)->toBe($blueprint)
                    ->and($blueprint->getSource()['type'])->toBe('file')
                    ->and($blueprint->getSource()['path'])->toBe('/path/to/file.csv');
            });
        });

        describe('sourceExecutable', function (): void {
            it('sets Executable source', function (): void {
                $blueprint = new DictionaryBlueprint('my_dict');

                $result = $blueprint->sourceExecutable('cat /data/file.json', 'JSONEachRow');

                expect($result)->toBe($blueprint)
                    ->and($blueprint->getSource()['type'])->toBe('executable')
                    ->and($blueprint->getSource()['command'])->toBe('cat /data/file.json');
            });
        });

        describe('sourceExecutablePool', function (): void {
            it('sets ExecutablePool source', function (): void {
                $blueprint = new DictionaryBlueprint('my_dict');

                $result = $blueprint->sourceExecutablePool('my_script.sh', 'TabSeparated', 4);

                expect($result)->toBe($blueprint)
                    ->and($blueprint->getSource()['type'])->toBe('executable_pool')
                    ->and($blueprint->getSource()['size'])->toBe(4);
            });
        });

        describe('sourceRedis', function (): void {
            it('sets Redis source', function (): void {
                $blueprint = new DictionaryBlueprint('my_dict');

                $result = $blueprint->sourceRedis('localhost', 6379);

                expect($result)->toBe($blueprint)
                    ->and($blueprint->getSource()['type'])->toBe('redis')
                    ->and($blueprint->getSource()['db_index'])->toBe(0);
            });

            it('sets Redis source with options', function (): void {
                $blueprint = new DictionaryBlueprint('my_dict');

                $blueprint->sourceRedis('localhost', 6379, 1, 'secret');

                expect($blueprint->getSource()['db_index'])->toBe(1)
                    ->and($blueprint->getSource()['password'])->toBe('secret');
            });
        });
    });

    describe('layouts', function (): void {
        describe('layoutFlat', function (): void {
            it('sets flat layout with defaults', function (): void {
                $blueprint = new DictionaryBlueprint('my_dict');

                $result = $blueprint->layoutFlat();

                expect($result)->toBe($blueprint)
                    ->and($blueprint->getLayout())->toBe([
                        'type' => 'flat',
                        'initial_array_size' => 1024,
                        'max_array_size' => 500000,
                    ]);
            });

            it('sets flat layout with custom sizes', function (): void {
                $blueprint = new DictionaryBlueprint('my_dict');

                $blueprint->layoutFlat(2048, 1000000);

                expect($blueprint->getLayout()['initial_array_size'])->toBe(2048)
                    ->and($blueprint->getLayout()['max_array_size'])->toBe(1000000);
            });
        });

        describe('layoutHashed', function (): void {
            it('sets hashed layout', function (): void {
                $blueprint = new DictionaryBlueprint('my_dict');

                $result = $blueprint->layoutHashed();

                expect($result)->toBe($blueprint)
                    ->and($blueprint->getLayout()['type'])->toBe('hashed');
            });

            it('sets hashed layout with shards', function (): void {
                $blueprint = new DictionaryBlueprint('my_dict');

                $blueprint->layoutHashed(4);

                expect($blueprint->getLayout()['shards'])->toBe(4);
            });
        });

        describe('layoutSparseHashed', function (): void {
            it('sets sparse_hashed layout', function (): void {
                $blueprint = new DictionaryBlueprint('my_dict');

                expect($blueprint->layoutSparseHashed()->getLayout()['type'])->toBe('sparse_hashed');
            });
        });

        describe('layoutHashedArray', function (): void {
            it('sets hashed_array layout', function (): void {
                $blueprint = new DictionaryBlueprint('my_dict');

                expect($blueprint->layoutHashedArray()->getLayout()['type'])->toBe('hashed_array');
            });
        });

        describe('layoutComplexKeyHashed', function (): void {
            it('sets complex_key_hashed layout', function (): void {
                $blueprint = new DictionaryBlueprint('my_dict');

                expect($blueprint->layoutComplexKeyHashed()->getLayout()['type'])->toBe('complex_key_hashed');
            });
        });

        describe('layoutComplexKeySparseHashed', function (): void {
            it('sets complex_key_sparse_hashed layout', function (): void {
                $blueprint = new DictionaryBlueprint('my_dict');

                expect($blueprint->layoutComplexKeySparseHashed()->getLayout()['type'])->toBe('complex_key_sparse_hashed');
            });
        });

        describe('layoutRangeHashed', function (): void {
            it('sets range_hashed layout', function (): void {
                $blueprint = new DictionaryBlueprint('my_dict');

                expect($blueprint->layoutRangeHashed()->getLayout()['type'])->toBe('range_hashed');
            });
        });

        describe('layoutComplexKeyRangeHashed', function (): void {
            it('sets complex_key_range_hashed layout', function (): void {
                $blueprint = new DictionaryBlueprint('my_dict');

                expect($blueprint->layoutComplexKeyRangeHashed()->getLayout()['type'])->toBe('complex_key_range_hashed');
            });
        });

        describe('layoutCache', function (): void {
            it('sets cache layout', function (): void {
                $blueprint = new DictionaryBlueprint('my_dict');

                $result = $blueprint->layoutCache(10000);

                expect($result)->toBe($blueprint)
                    ->and($blueprint->getLayout())->toBe([
                        'type' => 'cache',
                        'size_in_cells' => 10000,
                    ]);
            });
        });

        describe('layoutComplexKeyCache', function (): void {
            it('sets complex_key_cache layout', function (): void {
                $blueprint = new DictionaryBlueprint('my_dict');

                expect($blueprint->layoutComplexKeyCache(5000)->getLayout()['type'])->toBe('complex_key_cache');
            });
        });

        describe('layoutSSDCache', function (): void {
            it('sets ssd_cache layout', function (): void {
                $blueprint = new DictionaryBlueprint('my_dict');

                $result = $blueprint->layoutSSDCache('/path', 4096, 1073741824, 1048576, 1048576);

                expect($result)->toBe($blueprint)
                    ->and($blueprint->getLayout()['type'])->toBe('ssd_cache')
                    ->and($blueprint->getLayout()['path'])->toBe('/path');
            });
        });

        describe('layoutComplexKeySSDCache', function (): void {
            it('sets complex_key_ssd_cache layout', function (): void {
                $blueprint = new DictionaryBlueprint('my_dict');

                expect($blueprint->layoutComplexKeySSDCache('/path', 4096, 1073741824, 1048576, 1048576)->getLayout()['type'])
                    ->toBe('complex_key_ssd_cache');
            });
        });

        describe('layoutDirect', function (): void {
            it('sets direct layout', function (): void {
                $blueprint = new DictionaryBlueprint('my_dict');

                expect($blueprint->layoutDirect()->getLayout()['type'])->toBe('direct');
            });
        });

        describe('layoutComplexKeyDirect', function (): void {
            it('sets complex_key_direct layout', function (): void {
                $blueprint = new DictionaryBlueprint('my_dict');

                expect($blueprint->layoutComplexKeyDirect()->getLayout()['type'])->toBe('complex_key_direct');
            });
        });

        describe('layoutIPTrie', function (): void {
            it('sets ip_trie layout', function (): void {
                $blueprint = new DictionaryBlueprint('my_dict');

                expect($blueprint->layoutIPTrie()->getLayout()['type'])->toBe('ip_trie');
            });
        });
    });

    describe('lifetime', function (): void {
        it('sets lifetime with single value', function (): void {
            $blueprint = new DictionaryBlueprint('my_dict');

            $result = $blueprint->lifetime(300);

            expect($result)->toBe($blueprint)
                ->and($blueprint->getLifetime())->toBe([
                    'min' => 300,
                    'max' => 300,
                ]);
        });

        it('sets lifetime with min and max', function (): void {
            $blueprint = new DictionaryBlueprint('my_dict');

            $blueprint->lifetime(300, 600);

            expect($blueprint->getLifetime())->toBe([
                'min' => 300,
                'max' => 600,
            ]);
        });
    });

    describe('range', function (): void {
        it('sets range columns', function (): void {
            $blueprint = new DictionaryBlueprint('my_dict');

            $result = $blueprint->range('start_date', 'end_date');

            expect($result)->toBe($blueprint)
                ->and($blueprint->getRange())->toBe([
                    'min' => 'start_date',
                    'max' => 'end_date',
                ]);
        });
    });

    describe('fluent interface', function (): void {
        it('supports full dictionary definition', function (): void {
            $blueprint = new DictionaryBlueprint('my_dict');

            $result = $blueprint
                ->ifNotExists()
                ->onCluster('my_cluster')
                ->primaryKey('id')
                ->sourceClickHouse('source_table', 'source_db')
                ->layoutHashed()
                ->lifetime(300, 600)
                ->comment('My dictionary');

            $blueprint->attribute('name', 'String', '');
            $blueprint->attribute('value', 'UInt64', 0);

            expect($result)->toBe($blueprint)
                ->and($blueprint->shouldUseIfNotExists())->toBeTrue()
                ->and($blueprint->getOnCluster())->toBe('my_cluster')
                ->and($blueprint->getPrimaryKey())->toBe(['id'])
                ->and($blueprint->getSource()['type'])->toBe('clickhouse')
                ->and($blueprint->getLayout()['type'])->toBe('hashed')
                ->and($blueprint->getLifetime()['min'])->toBe(300)
                ->and($blueprint->getAttributes())->toHaveCount(2)
                ->and($blueprint->getComment())->toBe('My dictionary');
        });
    });
});

/**
 * CODE REVIEW NOTES for DictionaryBlueprint.php:
 *
 * 1. GOOD: Comprehensive support for all ClickHouse dictionary sources
 * 2. GOOD: Support for all dictionary layouts
 * 3. GOOD: Clean attribute definition with hierarchical/injective/expression support
 * 4. GOOD: Proper lifetime configuration with min/max
 *
 * POTENTIAL IMPROVEMENTS:
 * 1. Consider adding validation for source configurations
 * 2. Could add helper methods for common dictionary patterns
 * 3. The MongoDB source uses 'collection' while others use 'table' - this is correct but could be documented
 */
