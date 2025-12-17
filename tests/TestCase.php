<?php

declare(strict_types=1);

namespace Tests;

use Flarme\PhpClickhouse\Client;
use PHPUnit\Framework\TestCase as BaseTestCase;

abstract class TestCase extends BaseTestCase
{
    public protected(set) Client $client {
        get {
            return $this->client ?? $this->createClient();
        }
    }

    /**
     * Create a ClickHouse client connected to the test instance.
     */
    protected function createClient(?string $database = null): Client
    {
        return new Client(
            host: $_ENV['CLICKHOUSE_HOST'] ?? 'localhost',
            port: (int) ($_ENV['CLICKHOUSE_PORT'] ?? 8123),
            username: $_ENV['CLICKHOUSE_USERNAME'] ?? 'user',
            password: $_ENV['CLICKHOUSE_PASSWORD'] ?? 'password',
            database: $database ?? $_ENV['CLICKHOUSE_DATABASE'] ?? 'database',
            settings: ['wait_end_of_query' => true],
        );
    }

    /**
     * Create a ClickHouse client connected to the replica instance.
     */
    protected function createReplicaClient(?string $database = null): Client
    {
        return new Client(
            host: $_ENV['CLICKHOUSE_REP_HOST'] ?? 'localhost',
            port: (int) ($_ENV['CLICKHOUSE_REP_PORT'] ?? 8124),
            username: $_ENV['CLICKHOUSE_REP_USERNAME'] ?? 'user',
            password: $_ENV['CLICKHOUSE_REP_PASSWORD'] ?? 'password',
            database: $database ?? $_ENV['CLICKHOUSE_REP_DATABASE'] ?? 'database',
        );
    }

    /**
     * Get the test database name.
     */
    protected function getTestDatabase(): string
    {
        return $_ENV['CLICKHOUSE_DATABASE'] ?? 'database';
    }

    /**
     * Generate a unique table name for testing.
     */
    protected function uniqueTableName(string $prefix = 'test'): string
    {
        return $prefix . '_' . uniqid() . '_' . time();
    }
}
