<?php

declare(strict_types=1);

namespace Flarme\PhpClickhouse\Integrations\Laravel;

use Flarme\PhpClickhouse\Client;
use Illuminate\Contracts\Foundation\Application;
use InvalidArgumentException;

/**
 * Manages multiple ClickHouse connections.
 */
class ClickhouseManager
{
    /**
     * The application instance.
     */
    protected Application $app;

    /**
     * The active connection instances.
     *
     * @var array<string, Client>
     */
    protected array $connections = [];

    /**
     * Create a new ClickHouse manager instance.
     */
    public function __construct(Application $app)
    {
        $this->app = $app;
    }

    /**
     * Dynamically pass methods to the default connection.
     *
     * @param  array<mixed>  $parameters
     */
    public function __call(string $method, array $parameters): mixed
    {
        return $this->connection()->{$method}(...$parameters);
    }

    /**
     * Get a ClickHouse connection instance.
     */
    public function connection(?string $name = null): Client
    {
        $name ??= $this->getDefaultConnection();

        if ( ! isset($this->connections[$name])) {
            $this->connections[$name] = $this->makeConnection($name);
        }

        return $this->connections[$name];
    }

    /**
     * Get the default connection name.
     */
    public function getDefaultConnection(): string
    {
        return $this->app['config']['clickhouse.default'] ?? 'default';
    }

    /**
     * Set the default connection name.
     */
    public function setDefaultConnection(string $name): void
    {
        $this->app['config']['clickhouse.default'] = $name;
    }

    /**
     * Get all of the created connections.
     *
     * @return array<string, Client>
     */
    public function getConnections(): array
    {
        return $this->connections;
    }

    /**
     * Make a new ClickHouse connection.
     */
    protected function makeConnection($name): Client
    {
        $config = $this->getConfig($name);

        if ($config === null) {
            throw new InvalidArgumentException("ClickHouse connection [{$name}] is not configured.");
        }

        return new Client(
            host: $config['host'] ?? 'localhost',
            port: $config['port'] ?? 8123,
            username: $config['username'] ?? 'default',
            password: $config['password'] ?? '',
            database: $config['database'] ?? null,
            secure: $config['secure'] ?? false,
            options: $config['options'] ?? [],
            settings: $config['settings'] ?? [],
        );
    }

    /**
     * Get the configuration for a connection.
     *
     * @return array<string, mixed>|null
     */
    protected function getConfig(string $name): ?array
    {
        return $this->app['config']["clickhouse.connections.{$name}"];
    }
}
