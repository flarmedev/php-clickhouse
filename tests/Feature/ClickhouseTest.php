<?php

declare(strict_types=1);

use Flarme\PhpClickhouse\Exceptions\ClickhouseException;
use Flarme\PhpClickhouse\Response;

describe('Clickhouse connection', function (): void {
    it('connects to clickhouse', function (): void {
        expect($this->createClient()->execute('SELECT 1'))->toBeInstanceOf(Response::class);
    });

    it('throws exception on error', function (): void {
        expect(fn() => $this->createClient()->execute('SELEC'))->toThrow(ClickhouseException::class);
    });
});

describe('Clickhouse inserts', function (): void {
    beforeEach(fn() => $this->client->execute(
        <<<SQL
        CREATE TABLE IF NOT EXISTS mem_table (id UInt64, reference String, created_at DateTime64) ENGINE = Memory
        SQL
    ));
    afterEach(fn() => $this->client->execute('DROP TABLE IF EXISTS mem_table'));

    it('inserts data into tables', function (): void {
        $res = $this->client->insert('mem_table', [
            ['id' => 1, 'reference' => 'T-001', 'created_at' => time()],
            ['id' => 2, 'reference' => 'T-002', 'created_at' => time()],
            ['id' => 3, 'reference' => 'T-003', 'created_at' => time()],
        ]);

        expect((int) $res->summary['written_rows'])->toBe(3);
    });
});

describe('Clickhouse DDL queries', function (): void {
    beforeEach(fn() => $this->client->execute(
        <<<SQL
        CREATE TABLE IF NOT EXISTS ddl_table ON CLUSTER test_cluster (id UInt64)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/{table}','{replica}')
        ORDER BY id;
        SQL
    ));

    afterEach(fn() => $this->client->execute(
        <<<SQL
        DROP TABLE IF EXISTS ddl_table ON CLUSTER test_cluster
        SETTINGS database_atomic_wait_for_drop_and_detach_synchronously=1;
        SQL
    ));

    it('executes DDL queries', function (): void {
        $res = $this->client->execute('ALTER TABLE ddl_table ON CLUSTER test_cluster ADD COLUMN name String;');

        // DDL status
        expect($res->toArray())->toHaveCount(2);
    });

    it('inserts data in DDL queries', function (): void {
        $res = $this->client->insert('ddl_table', [
            ['id' => 1],
            ['id' => 2],
        ]);

        expect((int) $res->summary['written_rows'])->toBe(2);
    });
});
