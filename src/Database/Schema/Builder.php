<?php

declare(strict_types=1);

namespace Flarme\PhpClickhouse\Database\Schema;

use Closure;
use Flarme\PhpClickhouse\Contracts\ClientInterface;
use Flarme\PhpClickhouse\Database\Schema\Blueprints\DatabaseBlueprint;
use Flarme\PhpClickhouse\Database\Schema\Blueprints\DictionaryBlueprint;
use Flarme\PhpClickhouse\Database\Schema\Blueprints\TableBlueprint;
use Flarme\PhpClickhouse\Database\Schema\Blueprints\ViewBlueprint;

class Builder
{
    protected ?ClientInterface $client;

    protected Grammar $grammar;

    protected ?string $defaultOnCluster = null;

    public function __construct(?ClientInterface $client = null)
    {
        $this->client = $client;
        $this->grammar = new Grammar();
    }

    /**
     * Create a new schema builder instance with a client.
     */
    public static function connection(?ClientInterface $client = null): self
    {
        return new self($client);
    }

    /**
     * Set the default ON CLUSTER clause for all operations.
     */
    public function onCluster(?string $cluster): self
    {
        $this->defaultOnCluster = $cluster;

        return $this;
    }

    // ==================== DATABASE OPERATIONS ====================

    /**
     * Create a new database.
     */
    public function createDatabase(string $name, ?Closure $callback = null): string
    {
        $blueprint = new DatabaseBlueprint($name);

        if ($this->defaultOnCluster) {
            $blueprint->onCluster($this->defaultOnCluster);
        }

        if ($callback) {
            $callback($blueprint);
        }

        $sql = $this->grammar->compileCreateDatabase($blueprint);

        $this->execute($sql);

        return $sql;
    }

    /**
     * Drop a database.
     */
    public function dropDatabase(string $name): string
    {
        $sql = $this->grammar->compileDropDatabase($name, false, $this->defaultOnCluster);

        $this->execute($sql);

        return $sql;
    }

    /**
     * Drop a database if it exists.
     */
    public function dropDatabaseIfExists(string $name): string
    {
        $sql = $this->grammar->compileDropDatabase($name, true, $this->defaultOnCluster);

        $this->execute($sql);

        return $sql;
    }

    // ==================== TABLE OPERATIONS ====================

    /**
     * Create a new table.
     */
    public function create(string $table, Closure $callback): string
    {
        $blueprint = new TableBlueprint($table);

        if ($this->defaultOnCluster) {
            $blueprint->onCluster($this->defaultOnCluster);
        }

        $callback($blueprint);

        $sql = $this->grammar->compileCreate($blueprint);

        $this->execute($sql);

        return $sql;
    }

    /**
     * Alter an existing table.
     *
     * @return string[]
     */
    public function alter(string $table, Closure $callback): array
    {
        $blueprint = new TableBlueprint($table);

        if ($this->defaultOnCluster) {
            $blueprint->onCluster($this->defaultOnCluster);
        }

        $callback($blueprint);

        $statements = $this->grammar->compileAlter($blueprint);

        foreach ($statements as $sql) {
            $this->execute($sql);
        }

        return $statements;
    }

    /**
     * Drop a table.
     */
    public function drop(string $table): string
    {
        $sql = $this->grammar->compileDrop($table, false, $this->defaultOnCluster);

        $this->execute($sql);

        return $sql;
    }

    /**
     * Drop a table if it exists.
     */
    public function dropIfExists(string $table): string
    {
        $sql = $this->grammar->compileDrop($table, true, $this->defaultOnCluster);

        $this->execute($sql);

        return $sql;
    }

    /**
     * Rename a table.
     */
    public function rename(string $from, string $to): string
    {
        $sql = $this->grammar->compileRename($from, $to, $this->defaultOnCluster);

        $this->execute($sql);

        return $sql;
    }

    /**
     * Truncate a table.
     */
    public function truncate(string $table): string
    {
        $sql = $this->grammar->compileTruncate($table, $this->defaultOnCluster);

        $this->execute($sql);

        return $sql;
    }

    // ==================== VIEW OPERATIONS ====================

    /**
     * Create a new view.
     */
    public function createView(string $name, Closure $callback): string
    {
        $blueprint = new ViewBlueprint($name);

        if ($this->defaultOnCluster) {
            $blueprint->onCluster($this->defaultOnCluster);
        }

        $callback($blueprint);

        $sql = $this->grammar->compileCreateView($blueprint);

        $this->execute($sql);

        return $sql;
    }

    /**
     * Create a new materialized view.
     */
    public function createMaterializedView(string $name, Closure $callback): string
    {
        $blueprint = new ViewBlueprint($name);
        $blueprint->materialized();

        if ($this->defaultOnCluster) {
            $blueprint->onCluster($this->defaultOnCluster);
        }

        $callback($blueprint);

        $sql = $this->grammar->compileCreateView($blueprint);

        $this->execute($sql);

        return $sql;
    }

    /**
     * Drop a view.
     */
    public function dropView(string $name): string
    {
        $sql = $this->grammar->compileDropView($name, false, $this->defaultOnCluster);

        $this->execute($sql);

        return $sql;
    }

    /**
     * Drop a view if it exists.
     */
    public function dropViewIfExists(string $name): string
    {
        $sql = $this->grammar->compileDropView($name, true, $this->defaultOnCluster);

        $this->execute($sql);

        return $sql;
    }

    // ==================== DICTIONARY OPERATIONS ====================

    /**
     * Create a new dictionary.
     */
    public function createDictionary(string $name, Closure $callback): string
    {
        $blueprint = new DictionaryBlueprint($name);

        if ($this->defaultOnCluster) {
            $blueprint->onCluster($this->defaultOnCluster);
        }

        $callback($blueprint);

        $sql = $this->grammar->compileCreateDictionary($blueprint);

        $this->execute($sql);

        return $sql;
    }

    /**
     * Drop a dictionary.
     */
    public function dropDictionary(string $name): string
    {
        $sql = $this->grammar->compileDropDictionary($name, false, $this->defaultOnCluster);

        $this->execute($sql);

        return $sql;
    }

    /**
     * Drop a dictionary if it exists.
     */
    public function dropDictionaryIfExists(string $name): string
    {
        $sql = $this->grammar->compileDropDictionary($name, true, $this->defaultOnCluster);

        $this->execute($sql);

        return $sql;
    }

    // ==================== UTILITY METHODS ====================

    /**
     * Check if a table exists.
     */
    public function hasTable(string $table): bool
    {
        if ( ! $this->client) {
            return false;
        }

        $result = $this->client->execute("EXISTS TABLE {$table}");

        return (bool) $result->first()['result'];
    }

    /**
     * Check if a column exists in a table.
     */
    public function hasColumn(string $table, string $column): bool
    {
        if ( ! $this->client) {
            return false;
        }

        $result = $this->client->execute(
            "SELECT count() as cnt FROM system.columns WHERE table = ? AND name = ?",
            [$table, $column]
        );

        return $result->first()['cnt'] > 0;
    }

    /**
     * Check if a database exists.
     */
    public function hasDatabase(string $database): bool
    {
        if ( ! $this->client) {
            return false;
        }

        $result = $this->client->execute(
            "SELECT count() as cnt FROM system.databases WHERE name = ?",
            [$database]
        );

        return $result->first()['cnt'] > 0;
    }

    /**
     * Check if a view exists.
     */
    public function hasView(string $name): bool
    {
        if ( ! $this->client) {
            return false;
        }

        $result = $this->client->execute(
            "SELECT count() as cnt FROM system.tables WHERE name = ? AND engine LIKE '%View%'",
            [$name]
        );

        return $result->first()['cnt'] > 0;
    }

    /**
     * Check if a dictionary exists.
     */
    public function hasDictionary(string $name): bool
    {
        if ( ! $this->client) {
            return false;
        }

        $result = $this->client->execute(
            "SELECT count() as cnt FROM system.dictionaries WHERE name = ?",
            [$name]
        );

        return $result->first()['cnt'] > 0;
    }

    /**
     * Get the columns for a table.
     */
    public function getColumns(string $table): array
    {
        if ( ! $this->client) {
            return [];
        }

        $result = $this->client->execute(
            "SELECT name, type, default_kind, default_expression, comment FROM system.columns WHERE table = ? ORDER BY position",
            [$table]
        );

        return $result->toArray();
    }

    /**
     * Get the grammar instance.
     */
    public function getGrammar(): Grammar
    {
        return $this->grammar;
    }

    /**
     * Execute a SQL statement.
     */
    protected function execute(string $sql): void
    {
        if ($this->client) {
            $this->client->execute($sql);
        }
    }
}
