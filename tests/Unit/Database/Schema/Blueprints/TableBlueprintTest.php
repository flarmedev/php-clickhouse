<?php

declare(strict_types=1);

use Flarme\PhpClickhouse\Database\Schema\Blueprints\TableBlueprint;
use Flarme\PhpClickhouse\Database\Schema\Components\Column;

describe('TableBlueprint', function (): void {
    describe('construction', function (): void {
        it('initializes with empty state', function (): void {
            $blueprint = new TableBlueprint('users');

            expect($blueprint->getName())->toBe('users')
                ->and($blueprint->getColumns())->toBe([])
                ->and($blueprint->getCommands())->toBe([])
                ->and($blueprint->getEngine())->toBeNull()
                ->and($blueprint->getEngineParams())->toBe([])
                ->and($blueprint->getPartitionBy())->toBeNull()
                ->and($blueprint->getOrderBy())->toBe([])
                ->and($blueprint->getPrimaryKey())->toBe([])
                ->and($blueprint->getSampleBy())->toBeNull()
                ->and($blueprint->getTtl())->toBeNull()
                ->and($blueprint->getSettings())->toBe([])
                ->and($blueprint->getAsSelect())->toBeNull()
                ->and($blueprint->isTemporary())->toBeFalse();
        });
    });

    describe('column types', function (): void {
        describe('uuid', function (): void {
            it('adds UUID column', function (): void {
                $blueprint = new TableBlueprint('users');

                $column = $blueprint->uuid('id');

                expect($column)->toBeInstanceOf(Column::class)
                    ->and($column->getName())->toBe('id')
                    ->and($column->getType())->toBe('UUID')
                    ->and($blueprint->getColumns())->toHaveCount(1);
            });
        });

        describe('string', function (): void {
            it('adds String column', function (): void {
                $blueprint = new TableBlueprint('users');

                $column = $blueprint->string('name');

                expect($column->getType())->toBe('String');
            });
        });

        describe('fixedString', function (): void {
            it('adds FixedString column', function (): void {
                $blueprint = new TableBlueprint('users');

                $column = $blueprint->fixedString('code', 10);

                expect($column->getType())->toBe('FixedString(10)');
            });
        });

        describe('integer types', function (): void {
            it('adds Int8 column', function (): void {
                $blueprint = new TableBlueprint('test');
                expect($blueprint->int8('col')->getType())->toBe('Int8');
            });

            it('adds Int16 column', function (): void {
                $blueprint = new TableBlueprint('test');
                expect($blueprint->int16('col')->getType())->toBe('Int16');
            });

            it('adds Int32 column', function (): void {
                $blueprint = new TableBlueprint('test');
                expect($blueprint->int32('col')->getType())->toBe('Int32');
            });

            it('adds Int64 column', function (): void {
                $blueprint = new TableBlueprint('test');
                expect($blueprint->int64('col')->getType())->toBe('Int64');
            });

            it('adds Int128 column', function (): void {
                $blueprint = new TableBlueprint('test');
                expect($blueprint->int128('col')->getType())->toBe('Int128');
            });

            it('adds Int256 column', function (): void {
                $blueprint = new TableBlueprint('test');
                expect($blueprint->int256('col')->getType())->toBe('Int256');
            });

            it('adds UInt8 column', function (): void {
                $blueprint = new TableBlueprint('test');
                expect($blueprint->uint8('col')->getType())->toBe('UInt8');
            });

            it('adds UInt16 column', function (): void {
                $blueprint = new TableBlueprint('test');
                expect($blueprint->uint16('col')->getType())->toBe('UInt16');
            });

            it('adds UInt32 column', function (): void {
                $blueprint = new TableBlueprint('test');
                expect($blueprint->uint32('col')->getType())->toBe('UInt32');
            });

            it('adds UInt64 column', function (): void {
                $blueprint = new TableBlueprint('test');
                expect($blueprint->uint64('col')->getType())->toBe('UInt64');
            });

            it('adds UInt128 column', function (): void {
                $blueprint = new TableBlueprint('test');
                expect($blueprint->uint128('col')->getType())->toBe('UInt128');
            });

            it('adds UInt256 column', function (): void {
                $blueprint = new TableBlueprint('test');
                expect($blueprint->uint256('col')->getType())->toBe('UInt256');
            });
        });

        describe('float types', function (): void {
            it('adds Float32 column', function (): void {
                $blueprint = new TableBlueprint('test');
                expect($blueprint->float32('col')->getType())->toBe('Float32');
            });

            it('adds Float64 column', function (): void {
                $blueprint = new TableBlueprint('test');
                expect($blueprint->float64('col')->getType())->toBe('Float64');
            });
        });

        describe('decimal types', function (): void {
            it('adds Decimal column', function (): void {
                $blueprint = new TableBlueprint('test');
                expect($blueprint->decimal('price', 10, 2)->getType())->toBe('Decimal(10, 2)');
            });

            it('adds Decimal32 column', function (): void {
                $blueprint = new TableBlueprint('test');
                expect($blueprint->decimal32('price', 2)->getType())->toBe('Decimal32(2)');
            });

            it('adds Decimal64 column', function (): void {
                $blueprint = new TableBlueprint('test');
                expect($blueprint->decimal64('price', 4)->getType())->toBe('Decimal64(4)');
            });

            it('adds Decimal128 column', function (): void {
                $blueprint = new TableBlueprint('test');
                expect($blueprint->decimal128('price', 6)->getType())->toBe('Decimal128(6)');
            });

            it('adds Decimal256 column', function (): void {
                $blueprint = new TableBlueprint('test');
                expect($blueprint->decimal256('price', 8)->getType())->toBe('Decimal256(8)');
            });
        });

        describe('boolean', function (): void {
            it('adds Bool column', function (): void {
                $blueprint = new TableBlueprint('test');
                expect($blueprint->boolean('active')->getType())->toBe('Bool');
            });
        });

        describe('date types', function (): void {
            it('adds Date column', function (): void {
                $blueprint = new TableBlueprint('test');
                expect($blueprint->date('created_date')->getType())->toBe('Date');
            });

            it('adds Date32 column', function (): void {
                $blueprint = new TableBlueprint('test');
                expect($blueprint->date32('created_date')->getType())->toBe('Date32');
            });

            it('adds DateTime column', function (): void {
                $blueprint = new TableBlueprint('test');
                expect($blueprint->dateTime('created_at')->getType())->toBe('DateTime');
            });

            it('adds DateTime column with timezone', function (): void {
                $blueprint = new TableBlueprint('test');
                expect($blueprint->dateTime('created_at', 'UTC')->getType())->toBe("DateTime('UTC')");
            });

            it('adds DateTime64 column', function (): void {
                $blueprint = new TableBlueprint('test');
                expect($blueprint->dateTime64('created_at')->getType())->toBe('DateTime64(3)');
            });

            it('adds DateTime64 column with precision', function (): void {
                $blueprint = new TableBlueprint('test');
                expect($blueprint->dateTime64('created_at', 6)->getType())->toBe('DateTime64(6)');
            });

            it('adds DateTime64 column with timezone', function (): void {
                $blueprint = new TableBlueprint('test');
                expect($blueprint->dateTime64('created_at', 3, 'UTC')->getType())->toBe("DateTime64(3, 'UTC')");
            });
        });

        describe('enum types', function (): void {
            it('adds Enum8 column', function (): void {
                $blueprint = new TableBlueprint('test');
                $column = $blueprint->enum8('status', ['active' => 1, 'inactive' => 2]);
                expect($column->getType())->toBe("Enum8('active' = 1, 'inactive' = 2)");
            });

            it('adds Enum16 column', function (): void {
                $blueprint = new TableBlueprint('test');
                $column = $blueprint->enum16('status', ['active' => 1, 'inactive' => 2]);
                expect($column->getType())->toBe("Enum16('active' = 1, 'inactive' = 2)");
            });

            it('handles numeric keys in enum', function (): void {
                $blueprint = new TableBlueprint('test');
                $column = $blueprint->enum8('status', ['active', 'inactive']);
                expect($column->getType())->toBe("Enum8('active' = 0, 'inactive' = 1)");
            });
        });

        describe('array', function (): void {
            it('adds Array column', function (): void {
                $blueprint = new TableBlueprint('test');
                expect($blueprint->array('tags', 'String')->getType())->toBe('Array(String)');
            });
        });

        describe('tuple', function (): void {
            it('adds Tuple column', function (): void {
                $blueprint = new TableBlueprint('test');
                expect($blueprint->tuple('point', ['Float64', 'Float64'])->getType())->toBe('Tuple(Float64, Float64)');
            });
        });

        describe('map', function (): void {
            it('adds Map column', function (): void {
                $blueprint = new TableBlueprint('test');
                expect($blueprint->map('metadata', 'String', 'String')->getType())->toBe('Map(String, String)');
            });
        });

        describe('nested', function (): void {
            it('adds Nested column', function (): void {
                $blueprint = new TableBlueprint('test');
                $column = $blueprint->nested('items', function ($nested): void {
                    $nested->string('name');
                    $nested->uint32('quantity');
                });
                expect($column->getType())->toBe('Nested(name String, quantity UInt32)');
            });
        });

        describe('nullable', function (): void {
            it('adds Nullable column', function (): void {
                $blueprint = new TableBlueprint('test');
                expect($blueprint->nullable('optional', 'String')->getType())->toBe('Nullable(String)');
            });
        });

        describe('lowCardinality', function (): void {
            it('adds LowCardinality column', function (): void {
                $blueprint = new TableBlueprint('test');
                expect($blueprint->lowCardinality('status', 'String')->getType())->toBe('LowCardinality(String)');
            });
        });

        describe('ip types', function (): void {
            it('adds IPv4 column', function (): void {
                $blueprint = new TableBlueprint('test');
                expect($blueprint->ipv4('ip')->getType())->toBe('IPv4');
            });

            it('adds IPv6 column', function (): void {
                $blueprint = new TableBlueprint('test');
                expect($blueprint->ipv6('ip')->getType())->toBe('IPv6');
            });
        });

        describe('json', function (): void {
            it('adds JSON column', function (): void {
                $blueprint = new TableBlueprint('test');
                expect($blueprint->json('data')->getType())->toBe('JSON');
            });
        });

        describe('geo types', function (): void {
            it('adds Point column', function (): void {
                $blueprint = new TableBlueprint('test');
                expect($blueprint->point('location')->getType())->toBe('Point');
            });

            it('adds Ring column', function (): void {
                $blueprint = new TableBlueprint('test');
                expect($blueprint->ring('boundary')->getType())->toBe('Ring');
            });

            it('adds Polygon column', function (): void {
                $blueprint = new TableBlueprint('test');
                expect($blueprint->polygon('area')->getType())->toBe('Polygon');
            });

            it('adds MultiPolygon column', function (): void {
                $blueprint = new TableBlueprint('test');
                expect($blueprint->multiPolygon('regions')->getType())->toBe('MultiPolygon');
            });
        });

        describe('aggregate function types', function (): void {
            it('adds SimpleAggregateFunction column', function (): void {
                $blueprint = new TableBlueprint('test');
                expect($blueprint->simpleAggregateFunction('total', 'sum', 'UInt64')->getType())
                    ->toBe('SimpleAggregateFunction(sum, UInt64)');
            });

            it('adds AggregateFunction column', function (): void {
                $blueprint = new TableBlueprint('test');
                expect($blueprint->aggregateFunction('stats', 'quantile', 'Float64')->getType())
                    ->toBe('AggregateFunction(quantile, Float64)');
            });

            it('adds AggregateFunction column with multiple types', function (): void {
                $blueprint = new TableBlueprint('test');
                expect($blueprint->aggregateFunction('stats', 'argMax', 'String', 'DateTime')->getType())
                    ->toBe('AggregateFunction(argMax, String, DateTime)');
            });
        });
    });

    describe('engine configuration', function (): void {
        describe('engine', function (): void {
            it('sets custom engine', function (): void {
                $blueprint = new TableBlueprint('test');

                $result = $blueprint->engine('CustomEngine', 'param1', 'param2');

                expect($result)->toBe($blueprint)
                    ->and($blueprint->getEngine())->toBe('CustomEngine')
                    ->and($blueprint->getEngineParams())->toBe(['param1', 'param2']);
            });
        });

        describe('mergeTree', function (): void {
            it('sets MergeTree engine', function (): void {
                $blueprint = new TableBlueprint('test');
                expect($blueprint->mergeTree()->getEngine())->toBe('MergeTree');
            });
        });

        describe('replacingMergeTree', function (): void {
            it('sets ReplacingMergeTree engine', function (): void {
                $blueprint = new TableBlueprint('test');
                expect($blueprint->replacingMergeTree()->getEngine())->toBe('ReplacingMergeTree');
            });

            it('sets ReplacingMergeTree with version column', function (): void {
                $blueprint = new TableBlueprint('test');
                $blueprint->replacingMergeTree('version');
                expect($blueprint->getEngineParams())->toBe(['version']);
            });

            it('sets ReplacingMergeTree with version and isDeleted columns', function (): void {
                $blueprint = new TableBlueprint('test');
                $blueprint->replacingMergeTree('version', 'is_deleted');
                expect($blueprint->getEngineParams())->toBe(['version', 'is_deleted']);
            });
        });

        describe('summingMergeTree', function (): void {
            it('sets SummingMergeTree engine', function (): void {
                $blueprint = new TableBlueprint('test');
                expect($blueprint->summingMergeTree()->getEngine())->toBe('SummingMergeTree');
            });

            it('sets SummingMergeTree with columns', function (): void {
                $blueprint = new TableBlueprint('test');
                $blueprint->summingMergeTree(['amount', 'count']);
                expect($blueprint->getEngineParams())->toBe(['amount', 'count']);
            });
        });

        describe('aggregatingMergeTree', function (): void {
            it('sets AggregatingMergeTree engine', function (): void {
                $blueprint = new TableBlueprint('test');
                expect($blueprint->aggregatingMergeTree()->getEngine())->toBe('AggregatingMergeTree');
            });
        });

        describe('collapsingMergeTree', function (): void {
            it('sets CollapsingMergeTree engine', function (): void {
                $blueprint = new TableBlueprint('test');
                $blueprint->collapsingMergeTree('sign');
                expect($blueprint->getEngine())->toBe('CollapsingMergeTree')
                    ->and($blueprint->getEngineParams())->toBe(['sign']);
            });
        });

        describe('versionedCollapsingMergeTree', function (): void {
            it('sets VersionedCollapsingMergeTree engine', function (): void {
                $blueprint = new TableBlueprint('test');
                $blueprint->versionedCollapsingMergeTree('sign', 'version');
                expect($blueprint->getEngine())->toBe('VersionedCollapsingMergeTree')
                    ->and($blueprint->getEngineParams())->toBe(['sign', 'version']);
            });
        });

        describe('graphiteMergeTree', function (): void {
            it('sets GraphiteMergeTree engine', function (): void {
                $blueprint = new TableBlueprint('test');
                $blueprint->graphiteMergeTree('graphite_rollup');
                expect($blueprint->getEngine())->toBe('GraphiteMergeTree')
                    ->and($blueprint->getEngineParams())->toBe(['graphite_rollup']);
            });
        });

        describe('replicatedMergeTree', function (): void {
            it('sets ReplicatedMergeTree engine', function (): void {
                $blueprint = new TableBlueprint('test');
                $blueprint->replicatedMergeTree('/clickhouse/tables/{shard}/test', '{replica}');
                expect($blueprint->getEngine())->toBe('ReplicatedMergeTree')
                    ->and($blueprint->getEngineParams())->toBe(['/clickhouse/tables/{shard}/test', '{replica}']);
            });
        });

        describe('replicatedReplacingMergeTree', function (): void {
            it('sets ReplicatedReplacingMergeTree engine', function (): void {
                $blueprint = new TableBlueprint('test');
                $blueprint->replicatedReplacingMergeTree('/zk/path', 'replica1');
                expect($blueprint->getEngine())->toBe('ReplicatedReplacingMergeTree');
            });

            it('sets ReplicatedReplacingMergeTree with version column', function (): void {
                $blueprint = new TableBlueprint('test');
                $blueprint->replicatedReplacingMergeTree('/zk/path', 'replica1', 'version');
                expect($blueprint->getEngineParams())->toBe(['/zk/path', 'replica1', 'version']);
            });
        });

        describe('distributed', function (): void {
            it('sets Distributed engine', function (): void {
                $blueprint = new TableBlueprint('test');
                $blueprint->distributed('cluster', 'db', 'table');
                expect($blueprint->getEngine())->toBe('Distributed')
                    ->and($blueprint->getEngineParams())->toBe(['cluster', 'db', 'table']);
            });

            it('sets Distributed engine with sharding key', function (): void {
                $blueprint = new TableBlueprint('test');
                $blueprint->distributed('cluster', 'db', 'table', 'rand()');
                expect($blueprint->getEngineParams())->toBe(['cluster', 'db', 'table', 'rand()']);
            });
        });

        describe('memory', function (): void {
            it('sets Memory engine', function (): void {
                $blueprint = new TableBlueprint('test');
                expect($blueprint->memory()->getEngine())->toBe('Memory');
            });
        });

        describe('log engines', function (): void {
            it('sets Log engine', function (): void {
                $blueprint = new TableBlueprint('test');
                expect($blueprint->log()->getEngine())->toBe('Log');
            });

            it('sets TinyLog engine', function (): void {
                $blueprint = new TableBlueprint('test');
                expect($blueprint->tinyLog()->getEngine())->toBe('TinyLog');
            });

            it('sets StripeLog engine', function (): void {
                $blueprint = new TableBlueprint('test');
                expect($blueprint->stripeLog()->getEngine())->toBe('StripeLog');
            });
        });

        describe('buffer', function (): void {
            it('sets Buffer engine', function (): void {
                $blueprint = new TableBlueprint('test');
                $blueprint->buffer('db', 'table', 16, 10, 100, 10000, 1000000, 10000000, 100000000);
                expect($blueprint->getEngine())->toBe('Buffer')
                    ->and($blueprint->getEngineParams())->toBe(['db', 'table', 16, 10, 100, 10000, 1000000, 10000000, 100000000]);
            });
        });

        describe('null', function (): void {
            it('sets Null engine', function (): void {
                $blueprint = new TableBlueprint('test');
                expect($blueprint->null()->getEngine())->toBe('Null');
            });
        });

        describe('set', function (): void {
            it('sets Set engine', function (): void {
                $blueprint = new TableBlueprint('test');
                expect($blueprint->set()->getEngine())->toBe('Set');
            });
        });

        describe('join', function (): void {
            it('sets Join engine', function (): void {
                $blueprint = new TableBlueprint('test');
                $blueprint->join('ANY', 'LEFT', ['id']);
                expect($blueprint->getEngine())->toBe('Join')
                    ->and($blueprint->getEngineParams())->toBe(['ANY', 'LEFT', 'id']);
            });
        });

        describe('url', function (): void {
            it('sets URL engine', function (): void {
                $blueprint = new TableBlueprint('test');
                $blueprint->url('http://example.com/data', 'CSV');
                expect($blueprint->getEngine())->toBe('URL')
                    ->and($blueprint->getEngineParams())->toBe(['http://example.com/data', 'CSV']);
            });
        });

        describe('file', function (): void {
            it('sets File engine', function (): void {
                $blueprint = new TableBlueprint('test');
                $blueprint->file('CSV');
                expect($blueprint->getEngine())->toBe('File')
                    ->and($blueprint->getEngineParams())->toBe(['CSV']);
            });
        });

        describe('merge', function (): void {
            it('sets Merge engine', function (): void {
                $blueprint = new TableBlueprint('test');
                $blueprint->merge('db', '^logs_');
                expect($blueprint->getEngine())->toBe('Merge')
                    ->and($blueprint->getEngineParams())->toBe(['db', '^logs_']);
            });
        });

        describe('dictionary', function (): void {
            it('sets Dictionary engine', function (): void {
                $blueprint = new TableBlueprint('test');
                $blueprint->dictionary('my_dict');
                expect($blueprint->getEngine())->toBe('Dictionary')
                    ->and($blueprint->getEngineParams())->toBe(['my_dict']);
            });
        });
    });

    describe('table structure', function (): void {
        describe('partitionBy', function (): void {
            it('sets partition expression', function (): void {
                $blueprint = new TableBlueprint('test');

                $result = $blueprint->partitionBy('toYYYYMM(created_at)');

                expect($result)->toBe($blueprint)
                    ->and($blueprint->getPartitionBy())->toBe('toYYYYMM(created_at)');
            });
        });

        describe('orderBy', function (): void {
            it('sets order by columns from string', function (): void {
                $blueprint = new TableBlueprint('test');

                $result = $blueprint->orderBy('id');

                expect($result)->toBe($blueprint)
                    ->and($blueprint->getOrderBy())->toBe(['id']);
            });

            it('sets order by columns from array', function (): void {
                $blueprint = new TableBlueprint('test');

                $blueprint->orderBy(['id', 'created_at']);

                expect($blueprint->getOrderBy())->toBe(['id', 'created_at']);
            });
        });

        describe('primaryKey', function (): void {
            it('sets primary key from string', function (): void {
                $blueprint = new TableBlueprint('test');

                $result = $blueprint->primaryKey('id');

                expect($result)->toBe($blueprint)
                    ->and($blueprint->getPrimaryKey())->toBe(['id']);
            });

            it('sets primary key from array', function (): void {
                $blueprint = new TableBlueprint('test');

                $blueprint->primaryKey(['id', 'tenant_id']);

                expect($blueprint->getPrimaryKey())->toBe(['id', 'tenant_id']);
            });
        });

        describe('sampleBy', function (): void {
            it('sets sample by expression', function (): void {
                $blueprint = new TableBlueprint('test');

                $result = $blueprint->sampleBy('intHash32(user_id)');

                expect($result)->toBe($blueprint)
                    ->and($blueprint->getSampleBy())->toBe('intHash32(user_id)');
            });
        });

        describe('ttl', function (): void {
            it('sets TTL expression', function (): void {
                $blueprint = new TableBlueprint('test');

                $result = $blueprint->ttl('created_at + INTERVAL 1 MONTH');

                expect($result)->toBe($blueprint)
                    ->and($blueprint->getTtl())->toBe('created_at + INTERVAL 1 MONTH');
            });
        });

        describe('settings', function (): void {
            it('sets table settings', function (): void {
                $blueprint = new TableBlueprint('test');

                $result = $blueprint->settings(['index_granularity' => 8192]);

                expect($result)->toBe($blueprint)
                    ->and($blueprint->getSettings())->toBe(['index_granularity' => 8192]);
            });

            it('merges settings', function (): void {
                $blueprint = new TableBlueprint('test');

                $blueprint->settings(['a' => 1])->settings(['b' => 2]);

                expect($blueprint->getSettings())->toBe(['a' => 1, 'b' => 2]);
            });
        });

        describe('asSelect', function (): void {
            it('sets AS SELECT query', function (): void {
                $blueprint = new TableBlueprint('test');

                $result = $blueprint->asSelect('SELECT * FROM other_table');

                expect($result)->toBe($blueprint)
                    ->and($blueprint->getAsSelect())->toBe('SELECT * FROM other_table');
            });
        });

        describe('temporary', function (): void {
            it('sets temporary flag', function (): void {
                $blueprint = new TableBlueprint('test');

                $result = $blueprint->temporary();

                expect($result)->toBe($blueprint)
                    ->and($blueprint->isTemporary())->toBeTrue();
            });

            it('can disable temporary', function (): void {
                $blueprint = new TableBlueprint('test');

                $blueprint->temporary()->temporary(false);

                expect($blueprint->isTemporary())->toBeFalse();
            });
        });
    });

    describe('alter operations', function (): void {
        describe('addColumn', function (): void {
            it('adds column command', function (): void {
                $blueprint = new TableBlueprint('test');

                $column = $blueprint->addColumn('new_col', 'String');

                expect($column)->toBeInstanceOf(Column::class)
                    ->and($blueprint->getCommands())->toHaveCount(1)
                    ->and($blueprint->getCommands()[0]['type'])->toBe('addColumn');
            });
        });

        describe('dropColumn', function (): void {
            it('adds drop column command', function (): void {
                $blueprint = new TableBlueprint('test');

                $result = $blueprint->dropColumn('old_col');

                expect($result)->toBe($blueprint)
                    ->and($blueprint->getCommands()[0]['type'])->toBe('dropColumn')
                    ->and($blueprint->getCommands()[0]['name'])->toBe('old_col');
            });
        });

        describe('modifyColumn', function (): void {
            it('adds modify column command', function (): void {
                $blueprint = new TableBlueprint('test');

                $column = $blueprint->modifyColumn('col', 'UInt64');

                expect($column)->toBeInstanceOf(Column::class)
                    ->and($blueprint->getCommands()[0]['type'])->toBe('modifyColumn');
            });
        });

        describe('renameColumn', function (): void {
            it('adds rename column command', function (): void {
                $blueprint = new TableBlueprint('test');

                $result = $blueprint->renameColumn('old_name', 'new_name');

                expect($result)->toBe($blueprint)
                    ->and($blueprint->getCommands()[0]['type'])->toBe('renameColumn')
                    ->and($blueprint->getCommands()[0]['from'])->toBe('old_name')
                    ->and($blueprint->getCommands()[0]['to'])->toBe('new_name');
            });
        });

        describe('clearColumn', function (): void {
            it('adds clear column command', function (): void {
                $blueprint = new TableBlueprint('test');

                $result = $blueprint->clearColumn('col');

                expect($result)->toBe($blueprint)
                    ->and($blueprint->getCommands()[0]['type'])->toBe('clearColumn');
            });

            it('adds clear column command with partition', function (): void {
                $blueprint = new TableBlueprint('test');

                $blueprint->clearColumn('col', '202401');

                expect($blueprint->getCommands()[0]['partition'])->toBe('202401');
            });
        });

        describe('commentColumn', function (): void {
            it('adds comment column command', function (): void {
                $blueprint = new TableBlueprint('test');

                $result = $blueprint->commentColumn('col', 'My comment');

                expect($result)->toBe($blueprint)
                    ->and($blueprint->getCommands()[0]['type'])->toBe('commentColumn')
                    ->and($blueprint->getCommands()[0]['comment'])->toBe('My comment');
            });
        });

        describe('addIndex', function (): void {
            it('adds index command', function (): void {
                $blueprint = new TableBlueprint('test');

                $result = $blueprint->addIndex('idx_name', 'name', 'minmax', 4);

                expect($result)->toBe($blueprint)
                    ->and($blueprint->getCommands()[0]['type'])->toBe('addIndex')
                    ->and($blueprint->getCommands()[0]['name'])->toBe('idx_name')
                    ->and($blueprint->getCommands()[0]['expression'])->toBe('name')
                    ->and($blueprint->getCommands()[0]['indexType'])->toBe('minmax')
                    ->and($blueprint->getCommands()[0]['granularity'])->toBe(4);
            });
        });

        describe('dropIndex', function (): void {
            it('adds drop index command', function (): void {
                $blueprint = new TableBlueprint('test');

                $result = $blueprint->dropIndex('idx_name');

                expect($result)->toBe($blueprint)
                    ->and($blueprint->getCommands()[0]['type'])->toBe('dropIndex');
            });
        });

        describe('materializeIndex', function (): void {
            it('adds materialize index command', function (): void {
                $blueprint = new TableBlueprint('test');

                $result = $blueprint->materializeIndex('idx_name');

                expect($result)->toBe($blueprint)
                    ->and($blueprint->getCommands()[0]['type'])->toBe('materializeIndex');
            });

            it('adds materialize index command with partition', function (): void {
                $blueprint = new TableBlueprint('test');

                $blueprint->materializeIndex('idx_name', '202401');

                expect($blueprint->getCommands()[0]['partition'])->toBe('202401');
            });
        });

        describe('addProjection', function (): void {
            it('adds projection command', function (): void {
                $blueprint = new TableBlueprint('test');

                $result = $blueprint->addProjection('proj_name', 'SELECT * ORDER BY id');

                expect($result)->toBe($blueprint)
                    ->and($blueprint->getCommands()[0]['type'])->toBe('addProjection')
                    ->and($blueprint->getCommands()[0]['query'])->toBe('SELECT * ORDER BY id');
            });
        });

        describe('dropProjection', function (): void {
            it('adds drop projection command', function (): void {
                $blueprint = new TableBlueprint('test');

                $result = $blueprint->dropProjection('proj_name');

                expect($result)->toBe($blueprint)
                    ->and($blueprint->getCommands()[0]['type'])->toBe('dropProjection');
            });
        });

        describe('materializeProjection', function (): void {
            it('adds materialize projection command', function (): void {
                $blueprint = new TableBlueprint('test');

                $result = $blueprint->materializeProjection('proj_name');

                expect($result)->toBe($blueprint)
                    ->and($blueprint->getCommands()[0]['type'])->toBe('materializeProjection');
            });
        });

        describe('modifyTTL', function (): void {
            it('adds modify TTL command', function (): void {
                $blueprint = new TableBlueprint('test');

                $result = $blueprint->modifyTTL('created_at + INTERVAL 1 DAY');

                expect($result)->toBe($blueprint)
                    ->and($blueprint->getCommands()[0]['type'])->toBe('modifyTTL');
            });
        });

        describe('removeTTL', function (): void {
            it('adds remove TTL command', function (): void {
                $blueprint = new TableBlueprint('test');

                $result = $blueprint->removeTTL();

                expect($result)->toBe($blueprint)
                    ->and($blueprint->getCommands()[0]['type'])->toBe('removeTTL');
            });
        });

        describe('modifyOrderBy', function (): void {
            it('adds modify order by command', function (): void {
                $blueprint = new TableBlueprint('test');

                $result = $blueprint->modifyOrderBy(['id', 'created_at']);

                expect($result)->toBe($blueprint)
                    ->and($blueprint->getCommands()[0]['type'])->toBe('modifyOrderBy')
                    ->and($blueprint->getCommands()[0]['columns'])->toBe(['id', 'created_at']);
            });
        });

        describe('modifySetting', function (): void {
            it('adds modify setting command', function (): void {
                $blueprint = new TableBlueprint('test');

                $result = $blueprint->modifySetting('index_granularity', 4096);

                expect($result)->toBe($blueprint)
                    ->and($blueprint->getCommands()[0]['type'])->toBe('modifySetting')
                    ->and($blueprint->getCommands()[0]['name'])->toBe('index_granularity')
                    ->and($blueprint->getCommands()[0]['value'])->toBe(4096);
            });
        });

        describe('resetSetting', function (): void {
            it('adds reset setting command', function (): void {
                $blueprint = new TableBlueprint('test');

                $result = $blueprint->resetSetting('index_granularity');

                expect($result)->toBe($blueprint)
                    ->and($blueprint->getCommands()[0]['type'])->toBe('resetSetting');
            });
        });

        describe('delete', function (): void {
            it('adds delete command', function (): void {
                $blueprint = new TableBlueprint('test');

                $result = $blueprint->delete('id = 1');

                expect($result)->toBe($blueprint)
                    ->and($blueprint->getCommands()[0]['type'])->toBe('delete')
                    ->and($blueprint->getCommands()[0]['where'])->toBe('id = 1');
            });
        });

        describe('update', function (): void {
            it('adds update command', function (): void {
                $blueprint = new TableBlueprint('test');

                $result = $blueprint->update(['status' => 'inactive'], 'id = 1');

                expect($result)->toBe($blueprint)
                    ->and($blueprint->getCommands()[0]['type'])->toBe('update')
                    ->and($blueprint->getCommands()[0]['assignments'])->toBe(['status' => 'inactive'])
                    ->and($blueprint->getCommands()[0]['where'])->toBe('id = 1');
            });
        });
    });
});

/**
 * CODE REVIEW NOTES for TableBlueprint.php:
 *
 * 1. GOOD: Comprehensive support for all ClickHouse column types
 * 2. GOOD: Support for all major ClickHouse engines
 * 3. GOOD: Clean separation between CREATE and ALTER operations
 * 4. GOOD: Fluent interface throughout
 *
 * POTENTIAL IMPROVEMENTS:
 * 1. The formatEnumValues method could be more robust for edge cases
 * 2. Consider adding validation for engine parameters
 * 3. Some engine methods have inconsistent parameter handling (e.g., summingMergeTree accepts string|array)
 * 4. Could add helper methods for common column patterns (e.g., timestamps())
 */
