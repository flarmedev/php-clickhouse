<?php

declare(strict_types=1);

namespace Flarme\PhpClickhouse\Database\Schema;

use Flarme\PhpClickhouse\Contracts\ExpressionInterface;
use Flarme\PhpClickhouse\Database\Schema\Blueprints\DatabaseBlueprint;
use Flarme\PhpClickhouse\Database\Schema\Blueprints\DictionaryBlueprint;
use Flarme\PhpClickhouse\Database\Schema\Blueprints\TableBlueprint;
use Flarme\PhpClickhouse\Database\Schema\Blueprints\ViewBlueprint;
use Flarme\PhpClickhouse\Database\Schema\Components\Attribute;
use Flarme\PhpClickhouse\Database\Schema\Components\Column;
use Flarme\PhpClickhouse\Expressions\Raw;

class Grammar
{
    // ==================== DATABASE ====================

    /**
     * Compile a CREATE DATABASE statement.
     */
    public function compileCreateDatabase(DatabaseBlueprint $blueprint): string
    {
        $sql = 'CREATE DATABASE';

        if ($blueprint->shouldUseIfNotExists()) {
            $sql .= ' IF NOT EXISTS';
        }

        $sql .= ' ' . $this->wrap($blueprint->getName());

        if ($blueprint->getOnCluster()) {
            $sql .= ' ON CLUSTER ' . $this->wrap($blueprint->getOnCluster());
        }

        if ($blueprint->getEngine()) {
            $sql .= ' ENGINE = ' . $blueprint->getEngine();
        }

        if ($blueprint->getComment()) {
            $sql .= ' COMMENT ' . $this->quoteString($blueprint->getComment());
        }

        return $sql;
    }

    /**
     * Compile a DROP DATABASE statement.
     */
    public function compileDropDatabase(string $name, bool $ifExists = false, ?string $onCluster = null): string
    {
        $sql = 'DROP DATABASE';

        if ($ifExists) {
            $sql .= ' IF EXISTS';
        }

        $sql .= ' ' . $this->wrap($name);

        if ($onCluster) {
            $sql .= ' ON CLUSTER ' . $this->wrap($onCluster);
        }

        return $sql;
    }

    // ==================== TABLE ====================

    /**
     * Compile a CREATE TABLE statement.
     */
    public function compileCreate(TableBlueprint $blueprint): string
    {
        $sql = 'CREATE';

        if ($blueprint->isTemporary()) {
            $sql .= ' TEMPORARY';
        }

        $sql .= ' TABLE';

        if ($blueprint->shouldUseIfNotExists()) {
            $sql .= ' IF NOT EXISTS';
        }

        $sql .= ' ' . $this->wrap($blueprint->getName());

        if ($blueprint->getOnCluster()) {
            $sql .= ' ON CLUSTER ' . $this->wrap($blueprint->getOnCluster());
        }

        if ($blueprint->getAsTable()) {
            $sql .= ' AS ' . $this->wrapOrExpression($blueprint->getAsTable());
        }

        // Columns
        if (! $blueprint->getAsTable() && $blueprint->getColumns()) {
            $sql .= ' (' . $this->compileColumns($blueprint->getColumns()) . ')';
        }

        // Engine
        if ($blueprint->getEngine()) {
            $sql .= ' ENGINE = ' . $this->compileEngine($blueprint);
        }

        // Order By
        if ($blueprint->getOrderBy()) {
            $sql .= ' ORDER BY (' . implode(', ', array_map([$this, 'wrap'], $blueprint->getOrderBy())) . ')';
        }

        // Partition By
        if ($blueprint->getPartitionBy()) {
            $sql .= ' PARTITION BY ' . $blueprint->getPartitionBy();
        }

        // Primary Key
        if ($blueprint->getPrimaryKey()) {
            $sql .= ' PRIMARY KEY (' . implode(', ', array_map([$this, 'wrap'], $blueprint->getPrimaryKey())) . ')';
        }

        // Sample By
        if ($blueprint->getSampleBy()) {
            $sql .= ' SAMPLE BY ' . $blueprint->getSampleBy();
        }

        // TTL
        if ($blueprint->getTtl()) {
            $sql .= ' TTL ' . $blueprint->getTtl();
        }

        // Settings
        if ($blueprint->getSettings()) {
            $sql .= ' SETTINGS ' . $this->compileSettings($blueprint->getSettings());
        }

        // Comment
        if ($blueprint->getComment()) {
            $sql .= ' COMMENT ' . $this->quoteString($blueprint->getComment());
        }

        // AS SELECT
        if ($blueprint->getAsSelect()) {
            $sql .= ' AS ' . $blueprint->getAsSelect();
        }

        return $sql;
    }

    /**
     * Compile ALTER TABLE statements.
     *
     * @return string[]
     */
    public function compileAlter(TableBlueprint $blueprint): array
    {
        $statements = [];
        $tableName = $this->wrap($blueprint->getName());
        $onCluster = $blueprint->getOnCluster() ? ' ON CLUSTER ' . $this->wrap($blueprint->getOnCluster()) : '';

        foreach ($blueprint->getCommands() as $command) {
            $sql = "ALTER TABLE {$tableName}{$onCluster}";

            switch ($command['type']) {
                case 'addColumn':
                    $sql .= ' ADD COLUMN ' . $this->compileColumn($command['column']);
                    if ($command['column']->isFirst()) {
                        $sql .= ' FIRST';
                    } elseif ($command['column']->getAfter()) {
                        $sql .= ' AFTER ' . $this->wrap($command['column']->getAfter());
                    }
                    break;

                case 'dropColumn':
                    $sql .= ' DROP COLUMN ' . $this->wrap($command['name']);
                    break;

                case 'modifyColumn':
                    $sql .= ' MODIFY COLUMN ' . $this->compileColumn($command['column']);
                    break;

                case 'renameColumn':
                    $sql .= ' RENAME COLUMN ' . $this->wrap($command['from']) . ' TO ' . $this->wrap($command['to']);
                    break;

                case 'clearColumn':
                    $sql .= ' CLEAR COLUMN ' . $this->wrap($command['name']);
                    if ($command['partition']) {
                        $sql .= ' IN PARTITION ' . $command['partition'];
                    }
                    break;

                case 'commentColumn':
                    $sql .= ' COMMENT COLUMN ' . $this->wrap($command['name']) . ' ' . $this->quoteString($command['comment']);
                    break;

                case 'addIndex':
                    $sql .= ' ADD INDEX ' . $this->wrap($command['name'])
                        . ' ' . $command['expression']
                        . ' TYPE ' . $command['indexType']
                        . ' GRANULARITY ' . $command['granularity'];
                    break;

                case 'dropIndex':
                    $sql .= ' DROP INDEX ' . $this->wrap($command['name']);
                    break;

                case 'materializeIndex':
                    $sql .= ' MATERIALIZE INDEX ' . $this->wrap($command['name']);
                    if ($command['partition']) {
                        $sql .= ' IN PARTITION ' . $command['partition'];
                    }
                    break;

                case 'addProjection':
                    $sql .= ' ADD PROJECTION ' . $this->wrap($command['name']) . ' (' . $command['query'] . ')';
                    break;

                case 'dropProjection':
                    $sql .= ' DROP PROJECTION ' . $this->wrap($command['name']);
                    break;

                case 'materializeProjection':
                    $sql .= ' MATERIALIZE PROJECTION ' . $this->wrap($command['name']);
                    if ($command['partition']) {
                        $sql .= ' IN PARTITION ' . $command['partition'];
                    }
                    break;

                case 'modifyTTL':
                    $sql .= ' MODIFY TTL ' . $command['expression'];
                    break;

                case 'removeTTL':
                    $sql .= ' REMOVE TTL';
                    break;

                case 'modifyOrderBy':
                    $sql .= ' MODIFY ORDER BY (' . implode(', ', array_map([$this, 'wrap'], $command['columns'])) . ')';
                    break;

                case 'modifySetting':
                    $sql .= ' MODIFY SETTING ' . $command['name'] . ' = ' . $this->compileValue($command['value']);
                    break;

                case 'resetSetting':
                    $sql .= ' RESET SETTING ' . $command['name'];
                    break;

                case 'delete':
                    $sql .= ' DELETE WHERE ' . $command['where'];
                    break;

                case 'update':
                    $assignments = [];
                    foreach ($command['assignments'] as $column => $value) {
                        $assignments[] = $this->wrap($column) . ' = ' . $this->compileValue($value);
                    }
                    $sql .= ' UPDATE ' . implode(', ', $assignments) . ' WHERE ' . $command['where'];
                    break;

                default:
                    continue 2;
            }

            $statements[] = $sql;
        }

        return $statements;
    }

    /**
     * Compile a DROP TABLE statement.
     */
    public function compileDrop(string $table, bool $ifExists = false, ?string $onCluster = null): string
    {
        $sql = 'DROP TABLE';

        if ($ifExists) {
            $sql .= ' IF EXISTS';
        }

        $sql .= ' ' . $this->wrap($table);

        if ($onCluster) {
            $sql .= ' ON CLUSTER ' . $this->wrap($onCluster);
        }

        return $sql;
    }

    /**
     * Compile a RENAME TABLE statement.
     */
    public function compileRename(string $from, string $to, ?string $onCluster = null): string
    {
        $sql = 'RENAME TABLE ' . $this->wrap($from) . ' TO ' . $this->wrap($to);

        if ($onCluster) {
            $sql .= ' ON CLUSTER ' . $this->wrap($onCluster);
        }

        return $sql;
    }

    /**
     * Compile a TRUNCATE TABLE statement.
     */
    public function compileTruncate(string $table, ?string $onCluster = null): string
    {
        $sql = 'TRUNCATE TABLE ' . $this->wrap($table);

        if ($onCluster) {
            $sql .= ' ON CLUSTER ' . $this->wrap($onCluster);
        }

        return $sql;
    }

    // ==================== VIEW ====================

    /**
     * Compile a CREATE VIEW statement.
     */
    public function compileCreateView(ViewBlueprint $blueprint): string
    {
        $sql = 'CREATE';

        if ($blueprint->isMaterialized()) {
            $sql .= ' MATERIALIZED';
        }

        $sql .= ' VIEW';

        if ($blueprint->shouldUseIfNotExists()) {
            $sql .= ' IF NOT EXISTS';
        }

        $sql .= ' ' . $this->wrap($blueprint->getName());

        if ($blueprint->getOnCluster()) {
            $sql .= ' ON CLUSTER ' . $this->wrap($blueprint->getOnCluster());
        }

        // Refreshable materialized view
        if ($blueprint->isRefreshable() && $blueprint->getRefreshInterval()) {
            $sql .= ' REFRESH ' . $blueprint->getRefreshInterval();
        }

        // Target table for materialized view
        if ($blueprint->isMaterialized() && $blueprint->getToTable()) {
            $toTable = $blueprint->getToDatabase()
                ? $this->wrap($blueprint->getToDatabase()) . '.' . $this->wrap($blueprint->getToTable())
                : $this->wrap($blueprint->getToTable());
            $sql .= ' TO ' . $toTable;
        }

        // Explicit column definitions
        if ($blueprint->getColumns()) {
            $sql .= ' (' . $this->compileColumns($blueprint->getColumns()) . ')';
        }

        // Engine (for materialized views without TO clause)
        if ($blueprint->isMaterialized() && ! $blueprint->getToTable() && $blueprint->getEngine()) {
            $sql .= ' ENGINE = ' . $blueprint->getEngine();

            if ($blueprint->getEngineParams()) {
                $sql .= '(' . implode(', ', array_map([$this, 'compileValue'], $blueprint->getEngineParams())) . ')';
            }
        }

        // Order By (for materialized views)
        if ($blueprint->isMaterialized() && ! $blueprint->getToTable() && $blueprint->getOrderBy()) {
            $sql .= ' ORDER BY (' . implode(', ', array_map([$this, 'wrap'], $blueprint->getOrderBy())) . ')';
        }

        // Partition By (for materialized views)
        if ($blueprint->isMaterialized() && ! $blueprint->getToTable() && $blueprint->getPartitionBy()) {
            $sql .= ' PARTITION BY ' . $blueprint->getPartitionBy();
        }

        // Primary Key (for materialized views)
        if ($blueprint->isMaterialized() && ! $blueprint->getToTable() && $blueprint->getPrimaryKey()) {
            $sql .= ' PRIMARY KEY (' . implode(', ', array_map([$this, 'wrap'], $blueprint->getPrimaryKey())) . ')';
        }

        // TTL (for materialized views)
        if ($blueprint->isMaterialized() && ! $blueprint->getToTable() && $blueprint->getTtl()) {
            $sql .= ' TTL ' . $blueprint->getTtl();
        }

        // Settings (for materialized views)
        if ($blueprint->isMaterialized() && ! $blueprint->getToTable() && $blueprint->getSettings()) {
            $sql .= ' SETTINGS ' . $this->compileSettings($blueprint->getSettings());
        }

        // Populate
        if ($blueprint->isMaterialized() && $blueprint->shouldPopulate()) {
            $sql .= ' POPULATE';
        }

        // AS SELECT
        if ($blueprint->getAsSelect()) {
            $sql .= ' AS ' . $blueprint->getAsSelect();
        }

        // Comment
        if ($blueprint->getComment()) {
            $sql .= ' COMMENT ' . $this->quoteString($blueprint->getComment());
        }

        return $sql;
    }

    /**
     * Compile a DROP VIEW statement.
     */
    public function compileDropView(string $name, bool $ifExists = false, ?string $onCluster = null): string
    {
        $sql = 'DROP VIEW';

        if ($ifExists) {
            $sql .= ' IF EXISTS';
        }

        $sql .= ' ' . $this->wrap($name);

        if ($onCluster) {
            $sql .= ' ON CLUSTER ' . $this->wrap($onCluster);
        }

        return $sql;
    }

    // ==================== DICTIONARY ====================

    /**
     * Compile a CREATE DICTIONARY statement.
     */
    public function compileCreateDictionary(DictionaryBlueprint $blueprint): string
    {
        $sql = 'CREATE DICTIONARY';

        if ($blueprint->shouldUseIfNotExists()) {
            $sql .= ' IF NOT EXISTS';
        }

        $sql .= ' ' . $this->wrap($blueprint->getName());

        if ($blueprint->getOnCluster()) {
            $sql .= ' ON CLUSTER ' . $this->wrap($blueprint->getOnCluster());
        }

        // Structure
        $sql .= ' (' . $this->compileDictionaryStructure($blueprint) . ')';

        // Primary Key
        $sql .= ' PRIMARY KEY ' . implode(', ', array_map([$this, 'wrap'], $blueprint->getPrimaryKey()));

        // Source
        if ($blueprint->getSource()) {
            $sql .= ' SOURCE(' . $this->compileDictionarySource($blueprint->getSource()) . ')';
        }

        // Layout
        if ($blueprint->getLayout()) {
            $sql .= ' LAYOUT(' . $this->compileDictionaryLayout($blueprint->getLayout()) . ')';
        }

        // Lifetime
        if ($blueprint->getLifetime()) {
            $lifetime = $blueprint->getLifetime();
            if ($lifetime['min'] === $lifetime['max']) {
                $sql .= ' LIFETIME(' . $lifetime['min'] . ')';
            } else {
                $sql .= ' LIFETIME(MIN ' . $lifetime['min'] . ' MAX ' . $lifetime['max'] . ')';
            }
        }

        // Range
        if ($blueprint->getRange()) {
            $range = $blueprint->getRange();
            $sql .= ' RANGE(MIN ' . $this->wrap($range['min']) . ' MAX ' . $this->wrap($range['max']) . ')';
        }

        // Comment
        if ($blueprint->getComment()) {
            $sql .= ' COMMENT ' . $this->quoteString($blueprint->getComment());
        }

        return $sql;
    }

    /**
     * Compile a DROP DICTIONARY statement.
     */
    public function compileDropDictionary(string $name, bool $ifExists = false, ?string $onCluster = null): string
    {
        $sql = 'DROP DICTIONARY';

        if ($ifExists) {
            $sql .= ' IF EXISTS';
        }

        $sql .= ' ' . $this->wrap($name);

        if ($onCluster) {
            $sql .= ' ON CLUSTER ' . $this->wrap($onCluster);
        }

        return $sql;
    }

    // ==================== HELPERS ====================

    /**
     * Compile columns definition.
     *
     * @param  Column[]  $columns
     */
    protected function compileColumns(array $columns): string
    {
        $compiled = [];

        foreach ($columns as $column) {
            $compiled[] = $this->compileColumn($column);
        }

        return implode(', ', $compiled);
    }

    /**
     * Compile a single column definition.
     */
    protected function compileColumn(Column $column): string
    {
        $type = $column->getType();

        if ($column->isNullable() && ! str_starts_with($type, 'Nullable(')) {
            $type = "Nullable({$type})";
        }

        $sql = $this->wrap($column->getName()) . ' ' . $type;

        if ($column->hasDefault()) {
            $sql .= ' DEFAULT ' . $this->compileValue($column->getDefault());
        } elseif ($column->getDefaultExpression()) {
            $sql .= ' DEFAULT ' . $column->getDefaultExpression();
        }

        if ($column->getMaterialized()) {
            $sql .= ' MATERIALIZED ' . $column->getMaterialized();
        }

        if ($column->getAlias()) {
            $sql .= ' ALIAS ' . $column->getAlias();
        }

        if ($column->getCodec()) {
            $sql .= ' CODEC(' . $column->getCodec() . ')';
        }

        if ($column->getTtl()) {
            $sql .= ' TTL ' . $column->getTtl();
        }

        if ($column->getComment()) {
            $sql .= ' COMMENT ' . $this->quoteString($column->getComment());
        }

        return $sql;
    }

    /**
     * Compile engine definition.
     */
    protected function compileEngine(TableBlueprint $blueprint): string
    {
        $engine = $blueprint->getEngine();
        $params = $blueprint->getEngineParams();

        if (empty($params)) {
            return $engine;
        }

        $compiledParams = array_map([$this, 'compileValue'], $params);

        return $engine . '(' . implode(', ', $compiledParams) . ')';
    }

    /**
     * Compile settings.
     */
    protected function compileSettings(array $settings): string
    {
        $compiled = [];

        foreach ($settings as $key => $value) {
            $compiled[] = $key . ' = ' . $this->compileValue($value);
        }

        return implode(', ', $compiled);
    }

    /**
     * Compile dictionary structure.
     */
    protected function compileDictionaryStructure(DictionaryBlueprint $blueprint): string
    {
        $parts = [];

        // Primary key columns
        foreach ($blueprint->getPrimaryKey() as $key) {
            $parts[] = $this->wrap($key) . ' UInt64';
        }

        // Attributes
        foreach ($blueprint->getAttributes() as $attribute) {
            $parts[] = $this->compileAttribute($attribute);
        }

        return implode(', ', $parts);
    }

    /**
     * Compile a dictionary attribute.
     */
    protected function compileAttribute(Attribute $attribute): string
    {
        $sql = $this->wrap($attribute->getName()) . ' ' . $attribute->getType();

        if ($attribute->hasDefault()) {
            $sql .= ' DEFAULT ' . $this->compileValue($attribute->getDefault());
        }

        if ($attribute->isExpression()) {
            $sql .= ' EXPRESSION ' . $attribute->getExpression();
        }

        if ($attribute->isHierarchical()) {
            $sql .= ' HIERARCHICAL';
        }

        if ($attribute->isInjective()) {
            $sql .= ' INJECTIVE';
        }

        return $sql;
    }

    /**
     * Compile dictionary source.
     */
    protected function compileDictionarySource(array $source): string
    {
        $type = mb_strtoupper($source['type']);

        switch ($source['type']) {
            case 'clickhouse':
                $params = [];
                if (isset($source['database'])) {
                    $params[] = "db '{$source['database']}'";
                }
                $params[] = "table '{$source['table']}'";
                foreach ($source['options'] as $key => $value) {
                    $params[] = "{$key} " . $this->compileValue($value);
                }

                return "CLICKHOUSE(" . implode(' ', $params) . ')';

            case 'mysql':
                $params = [
                    "host '{$source['host']}'",
                    "port {$source['port']}",
                    "user '{$source['user']}'",
                    "password '{$source['password']}'",
                    "db '{$source['database']}'",
                    "table '{$source['table']}'",
                ];
                foreach ($source['options'] as $key => $value) {
                    $params[] = "{$key} " . $this->compileValue($value);
                }

                return 'MYSQL(' . implode(' ', $params) . ')';

            case 'postgresql':
                $params = [
                    "host '{$source['host']}'",
                    "port {$source['port']}",
                    "user '{$source['user']}'",
                    "password '{$source['password']}'",
                    "db '{$source['database']}'",
                    "table '{$source['table']}'",
                ];
                if (isset($source['schema'])) {
                    $params[] = "schema '{$source['schema']}'";
                }
                foreach ($source['options'] as $key => $value) {
                    $params[] = "{$key} " . $this->compileValue($value);
                }

                return 'POSTGRESQL(' . implode(' ', $params) . ')';

            case 'http':
                $params = [
                    "url '{$source['url']}'",
                    "format '{$source['format']}'",
                ];
                foreach ($source['options'] as $key => $value) {
                    $params[] = "{$key} " . $this->compileValue($value);
                }

                return 'HTTP(' . implode(' ', $params) . ')';

            case 'file':
                $params = [
                    "path '{$source['path']}'",
                    "format '{$source['format']}'",
                ];
                foreach ($source['options'] as $key => $value) {
                    $params[] = "{$key} " . $this->compileValue($value);
                }

                return 'FILE(' . implode(' ', $params) . ')';

            case 'executable':
                $params = [
                    "command '{$source['command']}'",
                    "format '{$source['format']}'",
                ];
                foreach ($source['options'] as $key => $value) {
                    $params[] = "{$key} " . $this->compileValue($value);
                }

                return 'EXECUTABLE(' . implode(' ', $params) . ')';

            default:
                return "{$type}()";
        }
    }

    /**
     * Compile dictionary layout.
     */
    protected function compileDictionaryLayout(array $layout): string
    {
        $type = mb_strtoupper($layout['type']);

        $params = [];
        foreach ($layout as $key => $value) {
            if ($key === 'type') {
                continue;
            }
            $params[] = mb_strtoupper($key) . ' ' . $value;
        }

        if (empty($params)) {
            return "{$type}()";
        }

        return "{$type}(" . implode(' ', $params) . ')';
    }

    /**
     * Wrap an identifier.
     */
    protected function wrap(string $value): string
    {
        if ($value === '*') {
            return $value;
        }

        return '`' . str_replace('`', '``', $value) . '`';
    }

    /**
     * Wrap an identifier or compile an expression.
     */
    protected function wrapOrExpression(string|Raw $value): string
    {
        if ($value instanceof Raw) {
            return (string) $value;
        }

        return $this->wrap($value);
    }

    /**
     * Quote a string value.
     */
    protected function quoteString(string $value): string
    {
        return "'" . str_replace("'", "''", $value) . "'";
    }

    /**
     * Compile a value for SQL.
     */
    protected function compileValue(mixed $value): string
    {
        if ($value instanceof ExpressionInterface) {
            return (string) $value;
        }

        if ($value === null) {
            return 'NULL';
        }

        if (is_bool($value)) {
            return $value ? '1' : '0';
        }

        if (is_int($value) || is_float($value)) {
            return (string) $value;
        }

        if (is_array($value)) {
            $compiled = array_map([$this, 'compileValue'], $value);

            return '[' . implode(', ', $compiled) . ']';
        }

        return $this->quoteString((string) $value);
    }
}
