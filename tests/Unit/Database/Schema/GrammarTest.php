<?php

declare(strict_types=1);

use Flarme\PhpClickhouse\Database\Schema\Blueprints\DatabaseBlueprint;
use Flarme\PhpClickhouse\Database\Schema\Blueprints\DictionaryBlueprint;
use Flarme\PhpClickhouse\Database\Schema\Blueprints\TableBlueprint;
use Flarme\PhpClickhouse\Database\Schema\Blueprints\ViewBlueprint;
use Flarme\PhpClickhouse\Database\Schema\Grammar;

describe('Schema Grammar', function () {
    function createSchemaGrammar(): Grammar
    {
        return new Grammar();
    }

    describe('compileCreateDatabase', function (): void {
        it('compiles basic CREATE DATABASE', function (): void {
            $grammar = createSchemaGrammar();
            $blueprint = new DatabaseBlueprint('test_db');

            $sql = $grammar->compileCreateDatabase($blueprint);

            expect($sql)->toBe('CREATE DATABASE `test_db`');
        });

        it('compiles CREATE DATABASE IF NOT EXISTS', function (): void {
            $grammar = createSchemaGrammar();
            $blueprint = (new DatabaseBlueprint('test_db'))->ifNotExists();

            $sql = $grammar->compileCreateDatabase($blueprint);

            expect($sql)->toBe('CREATE DATABASE IF NOT EXISTS `test_db`');
        });

        it('compiles CREATE DATABASE ON CLUSTER', function (): void {
            $grammar = createSchemaGrammar();
            $blueprint = (new DatabaseBlueprint('test_db'))->onCluster('my_cluster');

            $sql = $grammar->compileCreateDatabase($blueprint);

            expect($sql)->toBe('CREATE DATABASE `test_db` ON CLUSTER `my_cluster`');
        });

        it('compiles CREATE DATABASE with ENGINE', function (): void {
            $grammar = createSchemaGrammar();
            $blueprint = (new DatabaseBlueprint('test_db'))->atomic();

            $sql = $grammar->compileCreateDatabase($blueprint);

            expect($sql)->toBe('CREATE DATABASE `test_db` ENGINE = Atomic');
        });

        it('compiles CREATE DATABASE with COMMENT', function (): void {
            $grammar = createSchemaGrammar();
            $blueprint = (new DatabaseBlueprint('test_db'))->comment('Test database');

            $sql = $grammar->compileCreateDatabase($blueprint);

            expect($sql)->toBe("CREATE DATABASE `test_db` COMMENT 'Test database'");
        });

        it('compiles full CREATE DATABASE', function (): void {
            $grammar = createSchemaGrammar();
            $blueprint = (new DatabaseBlueprint('test_db'))
                ->ifNotExists()
                ->onCluster('cluster1')
                ->atomic()
                ->comment('My database');

            $sql = $grammar->compileCreateDatabase($blueprint);

            expect($sql)->toBe("CREATE DATABASE IF NOT EXISTS `test_db` ON CLUSTER `cluster1` ENGINE = Atomic COMMENT 'My database'");
        });
    });

    describe('compileDropDatabase', function (): void {
        it('compiles DROP DATABASE', function (): void {
            $grammar = createSchemaGrammar();

            $sql = $grammar->compileDropDatabase('test_db');

            expect($sql)->toBe('DROP DATABASE `test_db`');
        });

        it('compiles DROP DATABASE IF EXISTS', function (): void {
            $grammar = createSchemaGrammar();

            $sql = $grammar->compileDropDatabase('test_db', true);

            expect($sql)->toBe('DROP DATABASE IF EXISTS `test_db`');
        });

        it('compiles DROP DATABASE ON CLUSTER', function (): void {
            $grammar = createSchemaGrammar();

            $sql = $grammar->compileDropDatabase('test_db', false, 'my_cluster');

            expect($sql)->toBe('DROP DATABASE `test_db` ON CLUSTER `my_cluster`');
        });
    });

    describe('compileCreate (table)', function (): void {
        it('compiles basic CREATE TABLE', function (): void {
            $grammar = createSchemaGrammar();
            $blueprint = new TableBlueprint('users');
            $blueprint->uint64('id');
            $blueprint->string('name');
            $blueprint->mergeTree();
            $blueprint->orderBy('id');

            $sql = $grammar->compileCreate($blueprint);

            expect($sql)->toBe('CREATE TABLE `users` (`id` UInt64, `name` String) ENGINE = MergeTree ORDER BY (`id`)');
        });

        it('compiles CREATE TEMPORARY TABLE', function (): void {
            $grammar = createSchemaGrammar();
            $blueprint = new TableBlueprint('temp_users');
            $blueprint->temporary();
            $blueprint->uint64('id');
            $blueprint->memory();

            $sql = $grammar->compileCreate($blueprint);

            expect($sql)->toContain('CREATE TEMPORARY TABLE');
        });

        it('compiles CREATE TABLE IF NOT EXISTS', function (): void {
            $grammar = createSchemaGrammar();
            $blueprint = new TableBlueprint('users');
            $blueprint->ifNotExists();
            $blueprint->uint64('id');
            $blueprint->mergeTree();
            $blueprint->orderBy('id');

            $sql = $grammar->compileCreate($blueprint);

            expect($sql)->toContain('CREATE TABLE IF NOT EXISTS');
        });

        it('compiles CREATE TABLE ON CLUSTER', function (): void {
            $grammar = createSchemaGrammar();
            $blueprint = new TableBlueprint('users');
            $blueprint->onCluster('my_cluster');
            $blueprint->uint64('id');
            $blueprint->mergeTree();
            $blueprint->orderBy('id');

            $sql = $grammar->compileCreate($blueprint);

            expect($sql)->toContain('ON CLUSTER `my_cluster`');
        });

        it('compiles nullable column', function (): void {
            $grammar = createSchemaGrammar();
            $blueprint = new TableBlueprint('users');
            $blueprint->string('name')->nullable();
            $blueprint->mergeTree();
            $blueprint->orderBy('name');

            $sql = $grammar->compileCreate($blueprint);

            expect($sql)->toContain('`name` Nullable(String)');
        });

        it('compiles column with default value', function (): void {
            $grammar = createSchemaGrammar();
            $blueprint = new TableBlueprint('users');
            $blueprint->string('status')->default('active');
            $blueprint->mergeTree();
            $blueprint->orderBy('status');

            $sql = $grammar->compileCreate($blueprint);

            expect($sql)->toContain("`status` String DEFAULT 'active'");
        });

        it('compiles column with default expression', function (): void {
            $grammar = createSchemaGrammar();
            $blueprint = new TableBlueprint('users');
            $blueprint->dateTime('created_at')->defaultExpression('now()');
            $blueprint->mergeTree();
            $blueprint->orderBy('created_at');

            $sql = $grammar->compileCreate($blueprint);

            expect($sql)->toContain('`created_at` DateTime DEFAULT now()');
        });

        it('compiles column with MATERIALIZED', function (): void {
            $grammar = createSchemaGrammar();
            $blueprint = new TableBlueprint('users');
            $blueprint->dateTime('created_at');
            $blueprint->uint16('year')->materialized('toYear(created_at)');
            $blueprint->mergeTree();
            $blueprint->orderBy('created_at');

            $sql = $grammar->compileCreate($blueprint);

            expect($sql)->toContain('`year` UInt16 MATERIALIZED toYear(created_at)');
        });

        it('compiles column with ALIAS', function (): void {
            $grammar = createSchemaGrammar();
            $blueprint = new TableBlueprint('users');
            $blueprint->string('first_name');
            $blueprint->string('last_name');
            $blueprint->string('full_name')->alias("concat(first_name, ' ', last_name)");
            $blueprint->mergeTree();
            $blueprint->orderBy('first_name');

            $sql = $grammar->compileCreate($blueprint);

            expect($sql)->toContain("ALIAS concat(first_name, ' ', last_name)");
        });

        it('compiles column with CODEC', function (): void {
            $grammar = createSchemaGrammar();
            $blueprint = new TableBlueprint('users');
            $blueprint->string('data')->codec('ZSTD(3)');
            $blueprint->mergeTree();
            $blueprint->orderBy('data');

            $sql = $grammar->compileCreate($blueprint);

            expect($sql)->toContain('CODEC(ZSTD(3))');
        });

        it('compiles column with TTL', function (): void {
            $grammar = createSchemaGrammar();
            $blueprint = new TableBlueprint('users');
            $blueprint->dateTime('created_at');
            $blueprint->string('temp_data')->ttl('created_at + INTERVAL 1 DAY');
            $blueprint->mergeTree();
            $blueprint->orderBy('created_at');

            $sql = $grammar->compileCreate($blueprint);

            expect($sql)->toContain('TTL created_at + INTERVAL 1 DAY');
        });

        it('compiles column with COMMENT', function (): void {
            $grammar = createSchemaGrammar();
            $blueprint = new TableBlueprint('users');
            $blueprint->uint64('id')->comment('Primary key');
            $blueprint->mergeTree();
            $blueprint->orderBy('id');

            $sql = $grammar->compileCreate($blueprint);

            expect($sql)->toContain("COMMENT 'Primary key'");
        });

        it('compiles engine with parameters', function (): void {
            $grammar = createSchemaGrammar();
            $blueprint = new TableBlueprint('users');
            $blueprint->uint64('id');
            $blueprint->replacingMergeTree('version');
            $blueprint->orderBy('id');

            $sql = $grammar->compileCreate($blueprint);

            expect($sql)->toContain("ENGINE = ReplacingMergeTree('version')");
        });

        it('compiles PARTITION BY', function (): void {
            $grammar = createSchemaGrammar();
            $blueprint = new TableBlueprint('events');
            $blueprint->dateTime('created_at');
            $blueprint->mergeTree();
            $blueprint->orderBy('created_at');
            $blueprint->partitionBy('toYYYYMM(created_at)');

            $sql = $grammar->compileCreate($blueprint);

            expect($sql)->toContain('PARTITION BY toYYYYMM(created_at)');
        });

        it('compiles PRIMARY KEY', function (): void {
            $grammar = createSchemaGrammar();
            $blueprint = new TableBlueprint('users');
            $blueprint->uint64('id');
            $blueprint->mergeTree();
            $blueprint->orderBy(['id', 'name']);
            $blueprint->primaryKey('id');

            $sql = $grammar->compileCreate($blueprint);

            expect($sql)->toContain('PRIMARY KEY (`id`)');
        });

        it('compiles SAMPLE BY', function (): void {
            $grammar = createSchemaGrammar();
            $blueprint = new TableBlueprint('users');
            $blueprint->uint64('id');
            $blueprint->mergeTree();
            $blueprint->orderBy('id');
            $blueprint->sampleBy('intHash32(id)');

            $sql = $grammar->compileCreate($blueprint);

            expect($sql)->toContain('SAMPLE BY intHash32(id)');
        });

        it('compiles table TTL', function (): void {
            $grammar = createSchemaGrammar();
            $blueprint = new TableBlueprint('events');
            $blueprint->dateTime('created_at');
            $blueprint->mergeTree();
            $blueprint->orderBy('created_at');
            $blueprint->ttl('created_at + INTERVAL 1 MONTH');

            $sql = $grammar->compileCreate($blueprint);

            expect($sql)->toContain('TTL created_at + INTERVAL 1 MONTH');
        });

        it('compiles SETTINGS', function (): void {
            $grammar = createSchemaGrammar();
            $blueprint = new TableBlueprint('users');
            $blueprint->uint64('id');
            $blueprint->mergeTree();
            $blueprint->orderBy('id');
            $blueprint->settings(['index_granularity' => 8192]);

            $sql = $grammar->compileCreate($blueprint);

            expect($sql)->toContain('SETTINGS index_granularity = 8192');
        });

        it('compiles AS SELECT', function (): void {
            $grammar = createSchemaGrammar();
            $blueprint = new TableBlueprint('users_copy');
            $blueprint->mergeTree();
            $blueprint->orderBy('id');
            $blueprint->asSelect('SELECT * FROM users');

            $sql = $grammar->compileCreate($blueprint);

            expect($sql)->toContain('AS SELECT * FROM users');
        });

        it('compiles table COMMENT', function (): void {
            $grammar = createSchemaGrammar();
            $blueprint = new TableBlueprint('users');
            $blueprint->uint64('id');
            $blueprint->mergeTree();
            $blueprint->orderBy('id');
            $blueprint->comment('Users table');

            $sql = $grammar->compileCreate($blueprint);

            expect($sql)->toContain("COMMENT 'Users table'");
        });
    });

    describe('compileAlter', function (): void {
        it('compiles ADD COLUMN', function (): void {
            $grammar = createSchemaGrammar();
            $blueprint = new TableBlueprint('users');
            $blueprint->addColumn('email', 'String');

            $statements = $grammar->compileAlter($blueprint);

            expect($statements)->toHaveCount(1)
                ->and($statements[0])->toBe('ALTER TABLE `users` ADD COLUMN `email` String');
        });

        it('compiles ADD COLUMN FIRST', function (): void {
            $grammar = createSchemaGrammar();
            $blueprint = new TableBlueprint('users');
            $blueprint->addColumn('id', 'UInt64')->first();

            $statements = $grammar->compileAlter($blueprint);

            expect($statements[0])->toContain('FIRST');
        });

        it('compiles ADD COLUMN AFTER', function (): void {
            $grammar = createSchemaGrammar();
            $blueprint = new TableBlueprint('users');
            $blueprint->addColumn('email', 'String')->after('name');

            $statements = $grammar->compileAlter($blueprint);

            expect($statements[0])->toContain('AFTER `name`');
        });

        it('compiles DROP COLUMN', function (): void {
            $grammar = createSchemaGrammar();
            $blueprint = new TableBlueprint('users');
            $blueprint->dropColumn('old_column');

            $statements = $grammar->compileAlter($blueprint);

            expect($statements[0])->toBe('ALTER TABLE `users` DROP COLUMN `old_column`');
        });

        it('compiles MODIFY COLUMN', function (): void {
            $grammar = createSchemaGrammar();
            $blueprint = new TableBlueprint('users');
            $blueprint->modifyColumn('name', 'String')->nullable();

            $statements = $grammar->compileAlter($blueprint);

            expect($statements[0])->toBe('ALTER TABLE `users` MODIFY COLUMN `name` Nullable(String)');
        });

        it('compiles RENAME COLUMN', function (): void {
            $grammar = createSchemaGrammar();
            $blueprint = new TableBlueprint('users');
            $blueprint->renameColumn('old_name', 'new_name');

            $statements = $grammar->compileAlter($blueprint);

            expect($statements[0])->toBe('ALTER TABLE `users` RENAME COLUMN `old_name` TO `new_name`');
        });

        it('compiles CLEAR COLUMN', function (): void {
            $grammar = createSchemaGrammar();
            $blueprint = new TableBlueprint('users');
            $blueprint->clearColumn('data');

            $statements = $grammar->compileAlter($blueprint);

            expect($statements[0])->toBe('ALTER TABLE `users` CLEAR COLUMN `data`');
        });

        it('compiles CLEAR COLUMN IN PARTITION', function (): void {
            $grammar = createSchemaGrammar();
            $blueprint = new TableBlueprint('users');
            $blueprint->clearColumn('data', '202401');

            $statements = $grammar->compileAlter($blueprint);

            expect($statements[0])->toContain('IN PARTITION 202401');
        });

        it('compiles COMMENT COLUMN', function (): void {
            $grammar = createSchemaGrammar();
            $blueprint = new TableBlueprint('users');
            $blueprint->commentColumn('name', 'User name');

            $statements = $grammar->compileAlter($blueprint);

            expect($statements[0])->toBe("ALTER TABLE `users` COMMENT COLUMN `name` 'User name'");
        });

        it('compiles ADD INDEX', function (): void {
            $grammar = createSchemaGrammar();
            $blueprint = new TableBlueprint('users');
            $blueprint->addIndex('idx_name', 'name', 'minmax', 4);

            $statements = $grammar->compileAlter($blueprint);

            expect($statements[0])->toBe('ALTER TABLE `users` ADD INDEX `idx_name` name TYPE minmax GRANULARITY 4');
        });

        it('compiles DROP INDEX', function (): void {
            $grammar = createSchemaGrammar();
            $blueprint = new TableBlueprint('users');
            $blueprint->dropIndex('idx_name');

            $statements = $grammar->compileAlter($blueprint);

            expect($statements[0])->toBe('ALTER TABLE `users` DROP INDEX `idx_name`');
        });

        it('compiles MATERIALIZE INDEX', function (): void {
            $grammar = createSchemaGrammar();
            $blueprint = new TableBlueprint('users');
            $blueprint->materializeIndex('idx_name');

            $statements = $grammar->compileAlter($blueprint);

            expect($statements[0])->toBe('ALTER TABLE `users` MATERIALIZE INDEX `idx_name`');
        });

        it('compiles ADD PROJECTION', function (): void {
            $grammar = createSchemaGrammar();
            $blueprint = new TableBlueprint('users');
            $blueprint->addProjection('proj_name', 'SELECT * ORDER BY name');

            $statements = $grammar->compileAlter($blueprint);

            expect($statements[0])->toBe('ALTER TABLE `users` ADD PROJECTION `proj_name` (SELECT * ORDER BY name)');
        });

        it('compiles DROP PROJECTION', function (): void {
            $grammar = createSchemaGrammar();
            $blueprint = new TableBlueprint('users');
            $blueprint->dropProjection('proj_name');

            $statements = $grammar->compileAlter($blueprint);

            expect($statements[0])->toBe('ALTER TABLE `users` DROP PROJECTION `proj_name`');
        });

        it('compiles MODIFY TTL', function (): void {
            $grammar = createSchemaGrammar();
            $blueprint = new TableBlueprint('users');
            $blueprint->modifyTTL('created_at + INTERVAL 1 DAY');

            $statements = $grammar->compileAlter($blueprint);

            expect($statements[0])->toBe('ALTER TABLE `users` MODIFY TTL created_at + INTERVAL 1 DAY');
        });

        it('compiles REMOVE TTL', function (): void {
            $grammar = createSchemaGrammar();
            $blueprint = new TableBlueprint('users');
            $blueprint->removeTTL();

            $statements = $grammar->compileAlter($blueprint);

            expect($statements[0])->toBe('ALTER TABLE `users` REMOVE TTL');
        });

        it('compiles MODIFY ORDER BY', function (): void {
            $grammar = createSchemaGrammar();
            $blueprint = new TableBlueprint('users');
            $blueprint->modifyOrderBy(['id', 'created_at']);

            $statements = $grammar->compileAlter($blueprint);

            expect($statements[0])->toBe('ALTER TABLE `users` MODIFY ORDER BY (`id`, `created_at`)');
        });

        it('compiles MODIFY SETTING', function (): void {
            $grammar = createSchemaGrammar();
            $blueprint = new TableBlueprint('users');
            $blueprint->modifySetting('index_granularity', 4096);

            $statements = $grammar->compileAlter($blueprint);

            expect($statements[0])->toBe('ALTER TABLE `users` MODIFY SETTING index_granularity = 4096');
        });

        it('compiles RESET SETTING', function (): void {
            $grammar = createSchemaGrammar();
            $blueprint = new TableBlueprint('users');
            $blueprint->resetSetting('index_granularity');

            $statements = $grammar->compileAlter($blueprint);

            expect($statements[0])->toBe('ALTER TABLE `users` RESET SETTING index_granularity');
        });

        it('compiles DELETE', function (): void {
            $grammar = createSchemaGrammar();
            $blueprint = new TableBlueprint('users');
            $blueprint->delete('id = 1');

            $statements = $grammar->compileAlter($blueprint);

            expect($statements[0])->toBe('ALTER TABLE `users` DELETE WHERE id = 1');
        });

        it('compiles UPDATE', function (): void {
            $grammar = createSchemaGrammar();
            $blueprint = new TableBlueprint('users');
            $blueprint->update(['status' => 'inactive'], 'id = 1');

            $statements = $grammar->compileAlter($blueprint);

            expect($statements[0])->toBe("ALTER TABLE `users` UPDATE `status` = 'inactive' WHERE id = 1");
        });

        it('compiles ALTER ON CLUSTER', function (): void {
            $grammar = createSchemaGrammar();
            $blueprint = new TableBlueprint('users');
            $blueprint->onCluster('my_cluster');
            $blueprint->dropColumn('old_column');

            $statements = $grammar->compileAlter($blueprint);

            expect($statements[0])->toContain('ON CLUSTER `my_cluster`');
        });

        it('compiles multiple ALTER commands', function (): void {
            $grammar = createSchemaGrammar();
            $blueprint = new TableBlueprint('users');
            $blueprint->addColumn('email', 'String');
            $blueprint->dropColumn('old_column');

            $statements = $grammar->compileAlter($blueprint);

            expect($statements)->toHaveCount(2);
        });
    });

    describe('compileDrop', function (): void {
        it('compiles DROP TABLE', function (): void {
            $grammar = createSchemaGrammar();

            $sql = $grammar->compileDrop('users');

            expect($sql)->toBe('DROP TABLE `users`');
        });

        it('compiles DROP TABLE IF EXISTS', function (): void {
            $grammar = createSchemaGrammar();

            $sql = $grammar->compileDrop('users', true);

            expect($sql)->toBe('DROP TABLE IF EXISTS `users`');
        });

        it('compiles DROP TABLE ON CLUSTER', function (): void {
            $grammar = createSchemaGrammar();

            $sql = $grammar->compileDrop('users', false, 'my_cluster');

            expect($sql)->toBe('DROP TABLE `users` ON CLUSTER `my_cluster`');
        });
    });

    describe('compileRename', function (): void {
        it('compiles RENAME TABLE', function (): void {
            $grammar = createSchemaGrammar();

            $sql = $grammar->compileRename('old_table', 'new_table');

            expect($sql)->toBe('RENAME TABLE `old_table` TO `new_table`');
        });

        it('compiles RENAME TABLE ON CLUSTER', function (): void {
            $grammar = createSchemaGrammar();

            $sql = $grammar->compileRename('old_table', 'new_table', 'my_cluster');

            expect($sql)->toBe('RENAME TABLE `old_table` TO `new_table` ON CLUSTER `my_cluster`');
        });
    });

    describe('compileTruncate', function (): void {
        it('compiles TRUNCATE TABLE', function (): void {
            $grammar = createSchemaGrammar();

            $sql = $grammar->compileTruncate('users');

            expect($sql)->toBe('TRUNCATE TABLE `users`');
        });

        it('compiles TRUNCATE TABLE ON CLUSTER', function (): void {
            $grammar = createSchemaGrammar();

            $sql = $grammar->compileTruncate('users', 'my_cluster');

            expect($sql)->toBe('TRUNCATE TABLE `users` ON CLUSTER `my_cluster`');
        });
    });

    describe('compileCreateView', function (): void {
        it('compiles basic CREATE VIEW', function (): void {
            $grammar = createSchemaGrammar();
            $blueprint = new ViewBlueprint('my_view');
            $blueprint->as('SELECT * FROM users');

            $sql = $grammar->compileCreateView($blueprint);

            expect($sql)->toBe('CREATE VIEW `my_view` AS SELECT * FROM users');
        });

        it('compiles CREATE VIEW IF NOT EXISTS', function (): void {
            $grammar = createSchemaGrammar();
            $blueprint = new ViewBlueprint('my_view');
            $blueprint->ifNotExists();
            $blueprint->as('SELECT * FROM users');

            $sql = $grammar->compileCreateView($blueprint);

            expect($sql)->toContain('CREATE VIEW IF NOT EXISTS');
        });

        it('compiles CREATE MATERIALIZED VIEW', function (): void {
            $grammar = createSchemaGrammar();
            $blueprint = new ViewBlueprint('my_mv');
            $blueprint->materialized();
            $blueprint->to('target_table');
            $blueprint->as('SELECT * FROM source');

            $sql = $grammar->compileCreateView($blueprint);

            expect($sql)->toContain('CREATE MATERIALIZED VIEW')
                ->and($sql)->toContain('TO `target_table`');
        });

        it('compiles CREATE MATERIALIZED VIEW with database', function (): void {
            $grammar = createSchemaGrammar();
            $blueprint = new ViewBlueprint('my_mv');
            $blueprint->materialized();
            $blueprint->to('target_table', 'other_db');
            $blueprint->as('SELECT * FROM source');

            $sql = $grammar->compileCreateView($blueprint);

            expect($sql)->toContain('TO `other_db`.`target_table`');
        });

        it('compiles CREATE MATERIALIZED VIEW with storage', function (): void {
            $grammar = createSchemaGrammar();
            $blueprint = new ViewBlueprint('my_mv');
            $blueprint->materialized();
            $blueprint->mergeTree();
            $blueprint->orderBy('id');
            $blueprint->as('SELECT * FROM source');

            $sql = $grammar->compileCreateView($blueprint);

            expect($sql)->toContain('ENGINE = MergeTree')
                ->and($sql)->toContain('ORDER BY (`id`)');
        });

        it('compiles CREATE MATERIALIZED VIEW with POPULATE', function (): void {
            $grammar = createSchemaGrammar();
            $blueprint = new ViewBlueprint('my_mv');
            $blueprint->materialized();
            $blueprint->to('target');
            $blueprint->populate();
            $blueprint->as('SELECT * FROM source');

            $sql = $grammar->compileCreateView($blueprint);

            expect($sql)->toContain('POPULATE');
        });

        it('compiles refreshable materialized view', function (): void {
            $grammar = createSchemaGrammar();
            $blueprint = new ViewBlueprint('my_mv');
            $blueprint->materialized();
            $blueprint->refreshable('EVERY 1 HOUR');
            $blueprint->to('target');
            $blueprint->as('SELECT * FROM source');

            $sql = $grammar->compileCreateView($blueprint);

            expect($sql)->toContain('REFRESH EVERY 1 HOUR');
        });

        it('compiles view with explicit columns', function (): void {
            $grammar = createSchemaGrammar();
            $blueprint = new ViewBlueprint('my_view');
            $blueprint->column('id', 'UInt64');
            $blueprint->column('name', 'String');
            $blueprint->as('SELECT * FROM users');

            $sql = $grammar->compileCreateView($blueprint);

            expect($sql)->toContain('(`id` UInt64, `name` String)');
        });

        it('compiles view with COMMENT', function (): void {
            $grammar = createSchemaGrammar();
            $blueprint = new ViewBlueprint('my_view');
            $blueprint->as('SELECT * FROM users');
            $blueprint->comment('My view');

            $sql = $grammar->compileCreateView($blueprint);

            expect($sql)->toContain("COMMENT 'My view'");
        });
    });

    describe('compileDropView', function (): void {
        it('compiles DROP VIEW', function (): void {
            $grammar = createSchemaGrammar();

            $sql = $grammar->compileDropView('my_view');

            expect($sql)->toBe('DROP VIEW `my_view`');
        });

        it('compiles DROP VIEW IF EXISTS', function (): void {
            $grammar = createSchemaGrammar();

            $sql = $grammar->compileDropView('my_view', true);

            expect($sql)->toBe('DROP VIEW IF EXISTS `my_view`');
        });
    });

    describe('compileCreateDictionary', function (): void {
        it('compiles basic CREATE DICTIONARY', function (): void {
            $grammar = createSchemaGrammar();
            $blueprint = new DictionaryBlueprint('my_dict');
            $blueprint->primaryKey('id');
            $blueprint->attribute('name', 'String', '');
            $blueprint->sourceClickHouse('source_table');
            $blueprint->layoutFlat();
            $blueprint->lifetime(300);

            $sql = $grammar->compileCreateDictionary($blueprint);

            expect($sql)->toContain('CREATE DICTIONARY `my_dict`')
                ->and($sql)->toContain('PRIMARY KEY `id`')
                ->and($sql)->toContain('SOURCE(CLICKHOUSE(')
                ->and($sql)->toContain('LAYOUT(FLAT(')
                ->and($sql)->toContain('LIFETIME(300)');
        });

        it('compiles dictionary with lifetime range', function (): void {
            $grammar = createSchemaGrammar();
            $blueprint = new DictionaryBlueprint('my_dict');
            $blueprint->primaryKey('id');
            $blueprint->attribute('name', 'String');
            $blueprint->sourceClickHouse('source_table');
            $blueprint->layoutHashed();
            $blueprint->lifetime(300, 600);

            $sql = $grammar->compileCreateDictionary($blueprint);

            expect($sql)->toContain('LIFETIME(MIN 300 MAX 600)');
        });

        it('compiles dictionary with range', function (): void {
            $grammar = createSchemaGrammar();
            $blueprint = new DictionaryBlueprint('my_dict');
            $blueprint->primaryKey('id');
            $blueprint->attribute('value', 'UInt64');
            $blueprint->sourceClickHouse('source_table');
            $blueprint->layoutRangeHashed();
            $blueprint->lifetime(300);
            $blueprint->range('start_date', 'end_date');

            $sql = $grammar->compileCreateDictionary($blueprint);

            expect($sql)->toContain('RANGE(MIN `start_date` MAX `end_date`)');
        });

        it('compiles dictionary with hierarchical attribute', function (): void {
            $grammar = createSchemaGrammar();
            $blueprint = new DictionaryBlueprint('my_dict');
            $blueprint->primaryKey('id');
            $blueprint->hierarchicalAttribute('parent_id', 'UInt64', 0);
            $blueprint->sourceClickHouse('source_table');
            $blueprint->layoutFlat();
            $blueprint->lifetime(300);

            $sql = $grammar->compileCreateDictionary($blueprint);

            expect($sql)->toContain('HIERARCHICAL');
        });

        it('compiles dictionary with injective attribute', function (): void {
            $grammar = createSchemaGrammar();
            $blueprint = new DictionaryBlueprint('my_dict');
            $blueprint->primaryKey('id');
            $blueprint->injectiveAttribute('code', 'String', '');
            $blueprint->sourceClickHouse('source_table');
            $blueprint->layoutFlat();
            $blueprint->lifetime(300);

            $sql = $grammar->compileCreateDictionary($blueprint);

            expect($sql)->toContain('INJECTIVE');
        });

        it('compiles dictionary with expression attribute', function (): void {
            $grammar = createSchemaGrammar();
            $blueprint = new DictionaryBlueprint('my_dict');
            $blueprint->primaryKey('id');
            $blueprint->expressionAttribute('computed', 'UInt64', 'id * 2');
            $blueprint->sourceClickHouse('source_table');
            $blueprint->layoutFlat();
            $blueprint->lifetime(300);

            $sql = $grammar->compileCreateDictionary($blueprint);

            expect($sql)->toContain('EXPRESSION id * 2');
        });
    });

    describe('compileDropDictionary', function (): void {
        it('compiles DROP DICTIONARY', function (): void {
            $grammar = createSchemaGrammar();

            $sql = $grammar->compileDropDictionary('my_dict');

            expect($sql)->toBe('DROP DICTIONARY `my_dict`');
        });

        it('compiles DROP DICTIONARY IF EXISTS', function (): void {
            $grammar = createSchemaGrammar();

            $sql = $grammar->compileDropDictionary('my_dict', true);

            expect($sql)->toBe('DROP DICTIONARY IF EXISTS `my_dict`');
        });
    });
});

/**
 * CODE REVIEW NOTES for Schema Grammar.php:
 *
 * 1. GOOD: Comprehensive SQL generation for all ClickHouse schema operations
 * 2. GOOD: Proper escaping of identifiers and string values
 * 3. GOOD: Support for all column modifiers (MATERIALIZED, ALIAS, CODEC, TTL, etc.)
 * 4. GOOD: Clean separation of compilation methods
 *
 * POTENTIAL IMPROVEMENTS:
 * 1. Consider adding validation for generated SQL
 * 2. Some dictionary source compilation could be more DRY
 * 3. The compileValue method handles arrays recursively which is good
 * 4. Consider adding SQL formatting options for readability
 */
