<?php

declare(strict_types=1);

namespace Flarme\PhpClickhouse\Database\Schema\Blueprints;

use Closure;
use Flarme\PhpClickhouse\Database\Schema\Components\Column;
use Flarme\PhpClickhouse\Expressions\Raw;

class TableBlueprint extends Blueprint
{
    /** @var Column[] */
    protected array $columns = [];

    /** @var array<string, mixed> */
    protected array $commands = [];

    protected ?string $engine = null;

    protected array $engineParams = [];

    protected ?string $partitionBy = null;

    protected array $orderBy = [];

    protected array $primaryKey = [];

    protected ?string $sampleBy = null;

    protected ?string $ttl = null;

    protected array $settings = [];

    protected ?string $asSelect = null;

    protected bool $temporary = false;

    // ==================== GETTERS ====================

    /** @return Column[] */
    public function getColumns(): array
    {
        return $this->columns;
    }

    public function getCommands(): array
    {
        return $this->commands;
    }

    public function getEngine(): ?string
    {
        return $this->engine;
    }

    public function getEngineParams(): array
    {
        return $this->engineParams;
    }

    public function getPartitionBy(): ?string
    {
        return $this->partitionBy;
    }

    public function getOrderBy(): array
    {
        return $this->orderBy;
    }

    public function getPrimaryKey(): array
    {
        return $this->primaryKey;
    }

    public function getSampleBy(): ?string
    {
        return $this->sampleBy;
    }

    public function getTtl(): ?string
    {
        return $this->ttl;
    }

    public function getSettings(): array
    {
        return $this->settings;
    }

    public function getAsSelect(): ?string
    {
        return $this->asSelect;
    }

    public function isTemporary(): bool
    {
        return $this->temporary;
    }

    /**
     * Add a UUID column.
     */
    public function uuid(string $name): Column
    {
        return $this->addColumnDefinition($name, 'UUID');
    }

    /**
     * Add a String column.
     */
    public function string(string $name): Column
    {
        return $this->addColumnDefinition($name, 'String');
    }

    /**
     * Add a FixedString column.
     */
    public function fixedString(string $name, int $length): Column
    {
        return $this->addColumnDefinition($name, "FixedString({$length})");
    }

    /**
     * Add an Int8 column.
     */
    public function int8(string $name): Column
    {
        return $this->addColumnDefinition($name, 'Int8');
    }

    /**
     * Add an Int16 column.
     */
    public function int16(string $name): Column
    {
        return $this->addColumnDefinition($name, 'Int16');
    }

    /**
     * Add an Int32 column.
     */
    public function int32(string $name): Column
    {
        return $this->addColumnDefinition($name, 'Int32');
    }

    /**
     * Add an Int64 column.
     */
    public function int64(string $name): Column
    {
        return $this->addColumnDefinition($name, 'Int64');
    }

    /**
     * Add an Int128 column.
     */
    public function int128(string $name): Column
    {
        return $this->addColumnDefinition($name, 'Int128');
    }

    /**
     * Add an Int256 column.
     */
    public function int256(string $name): Column
    {
        return $this->addColumnDefinition($name, 'Int256');
    }

    /**
     * Add a UInt8 column.
     */
    public function uint8(string $name): Column
    {
        return $this->addColumnDefinition($name, 'UInt8');
    }

    /**
     * Add a UInt16 column.
     */
    public function uint16(string $name): Column
    {
        return $this->addColumnDefinition($name, 'UInt16');
    }

    /**
     * Add a UInt32 column.
     */
    public function uint32(string $name): Column
    {
        return $this->addColumnDefinition($name, 'UInt32');
    }

    /**
     * Add a UInt64 column.
     */
    public function uint64(string $name): Column
    {
        return $this->addColumnDefinition($name, 'UInt64');
    }

    /**
     * Add a UInt128 column.
     */
    public function uint128(string $name): Column
    {
        return $this->addColumnDefinition($name, 'UInt128');
    }

    /**
     * Add a UInt256 column.
     */
    public function uint256(string $name): Column
    {
        return $this->addColumnDefinition($name, 'UInt256');
    }

    /**
     * Add a Float32 column.
     */
    public function float32(string $name): Column
    {
        return $this->addColumnDefinition($name, 'Float32');
    }

    /**
     * Add a Float64 column.
     */
    public function float64(string $name): Column
    {
        return $this->addColumnDefinition($name, 'Float64');
    }

    /**
     * Add a Decimal column.
     */
    public function decimal(string $name, int $precision, int $scale): Column
    {
        return $this->addColumnDefinition($name, "Decimal({$precision}, {$scale})");
    }

    /**
     * Add a Decimal32 column.
     */
    public function decimal32(string $name, int $scale): Column
    {
        return $this->addColumnDefinition($name, "Decimal32({$scale})");
    }

    /**
     * Add a Decimal64 column.
     */
    public function decimal64(string $name, int $scale): Column
    {
        return $this->addColumnDefinition($name, "Decimal64({$scale})");
    }

    /**
     * Add a Decimal128 column.
     */
    public function decimal128(string $name, int $scale): Column
    {
        return $this->addColumnDefinition($name, "Decimal128({$scale})");
    }

    /**
     * Add a Decimal256 column.
     */
    public function decimal256(string $name, int $scale): Column
    {
        return $this->addColumnDefinition($name, "Decimal256({$scale})");
    }

    /**
     * Add a Bool column.
     */
    public function boolean(string $name): Column
    {
        return $this->addColumnDefinition($name, 'Bool');
    }

    /**
     * Add a Date column.
     */
    public function date(string $name): Column
    {
        return $this->addColumnDefinition($name, 'Date');
    }

    /**
     * Add a Date32 column.
     */
    public function date32(string $name): Column
    {
        return $this->addColumnDefinition($name, 'Date32');
    }

    /**
     * Add a DateTime column.
     */
    public function dateTime(string $name, ?string $timezone = null): Column
    {
        $type = $timezone ? "DateTime('{$timezone}')" : 'DateTime';

        return $this->addColumnDefinition($name, $type);
    }

    /**
     * Add a DateTime64 column.
     */
    public function dateTime64(string $name, int $precision = 3, ?string $timezone = null): Column
    {
        $type = $timezone ? "DateTime64({$precision}, '{$timezone}')" : "DateTime64({$precision})";

        return $this->addColumnDefinition($name, $type);
    }

    /**
     * Add an Enum8 column.
     */
    public function enum8(string $name, array $values): Column
    {
        $enumValues = $this->formatEnumValues($values);

        return $this->addColumnDefinition($name, "Enum8({$enumValues})");
    }

    /**
     * Add an Enum16 column.
     */
    public function enum16(string $name, array $values): Column
    {
        $enumValues = $this->formatEnumValues($values);

        return $this->addColumnDefinition($name, "Enum16({$enumValues})");
    }

    /**
     * Add an Array column.
     */
    public function array(string $name, string $type): Column
    {
        return $this->addColumnDefinition($name, "Array({$type})");
    }

    /**
     * Add a Tuple column.
     */
    public function tuple(string $name, array $types): Column
    {
        $tupleTypes = implode(', ', $types);

        return $this->addColumnDefinition($name, "Tuple({$tupleTypes})");
    }

    /**
     * Add a Map column.
     */
    public function map(string $name, string $keyType, string $valueType): Column
    {
        return $this->addColumnDefinition($name, "Map({$keyType}, {$valueType})");
    }

    /**
     * Add a Nested column.
     */
    public function nested(string $name, Closure $callback): Column
    {
        $nestedBlueprint = new self($name);
        $callback($nestedBlueprint);

        $nestedColumns = [];
        foreach ($nestedBlueprint->getColumns() as $column) {
            $nestedColumns[] = $column->getName() . ' ' . $column->getType();
        }

        $nestedDefinition = implode(', ', $nestedColumns);

        return $this->addColumnDefinition($name, "Nested({$nestedDefinition})");
    }

    /**
     * Add a Nullable column.
     */
    public function nullable(string $name, string $type): Column
    {
        return $this->addColumnDefinition($name, "Nullable({$type})");
    }

    /**
     * Add a LowCardinality column.
     */
    public function lowCardinality(string $name, string $type): Column
    {
        return $this->addColumnDefinition($name, "LowCardinality({$type})");
    }

    /**
     * Add an IPv4 column.
     */
    public function ipv4(string $name): Column
    {
        return $this->addColumnDefinition($name, 'IPv4');
    }

    /**
     * Add an IPv6 column.
     */
    public function ipv6(string $name): Column
    {
        return $this->addColumnDefinition($name, 'IPv6');
    }

    /**
     * Add a JSON column.
     */
    public function json(string $name): Column
    {
        return $this->addColumnDefinition($name, 'JSON');
    }

    /**
     * Add a Point column.
     */
    public function point(string $name): Column
    {
        return $this->addColumnDefinition($name, 'Point');
    }

    /**
     * Add a Ring column.
     */
    public function ring(string $name): Column
    {
        return $this->addColumnDefinition($name, 'Ring');
    }

    /**
     * Add a Polygon column.
     */
    public function polygon(string $name): Column
    {
        return $this->addColumnDefinition($name, 'Polygon');
    }

    /**
     * Add a MultiPolygon column.
     */
    public function multiPolygon(string $name): Column
    {
        return $this->addColumnDefinition($name, 'MultiPolygon');
    }

    /**
     * Add a SimpleAggregateFunction column.
     */
    public function simpleAggregateFunction(string $name, string $function, string $type): Column
    {
        return $this->addColumnDefinition($name, "SimpleAggregateFunction({$function}, {$type})");
    }

    /**
     * Add an AggregateFunction column.
     */
    public function aggregateFunction(string $name, string $function, string ...$types): Column
    {
        $typeList = implode(', ', $types);

        return $this->addColumnDefinition($name, "AggregateFunction({$function}, {$typeList})");
    }

    // ==================== ENGINE CONFIGURATION ====================

    /**
     * Set the table engine.
     */
    public function engine(string $engine, mixed ...$params): self
    {
        $this->engine = $engine;
        $this->engineParams = $params;

        return $this;
    }

    /**
     * Use MergeTree engine.
     */
    public function mergeTree(): self
    {
        return $this->engine('MergeTree');
    }

    /**
     * Use ReplacingMergeTree engine.
     */
    public function replacingMergeTree(?string $versionColumn = null, ?string $isDeletedColumn = null): self
    {
        $params = array_filter([$versionColumn, $isDeletedColumn]);

        return $this->engine('ReplacingMergeTree', ...$params);
    }

    /**
     * Use SummingMergeTree engine.
     */
    public function summingMergeTree(string|array $columns = []): self
    {
        $columns = is_array($columns) ? $columns : [$columns];

        return $this->engine('SummingMergeTree', ...$columns);
    }

    /**
     * Use AggregatingMergeTree engine.
     */
    public function aggregatingMergeTree(): self
    {
        return $this->engine('AggregatingMergeTree');
    }

    /**
     * Use CollapsingMergeTree engine.
     */
    public function collapsingMergeTree(string $signColumn): self
    {
        return $this->engine('CollapsingMergeTree', $signColumn);
    }

    /**
     * Use VersionedCollapsingMergeTree engine.
     */
    public function versionedCollapsingMergeTree(string $signColumn, string $versionColumn): self
    {
        return $this->engine('VersionedCollapsingMergeTree', $signColumn, $versionColumn);
    }

    /**
     * Use GraphiteMergeTree engine.
     */
    public function graphiteMergeTree(string $configSection): self
    {
        return $this->engine('GraphiteMergeTree', $configSection);
    }

    /**
     * Use ReplicatedMergeTree engine.
     */
    public function replicatedMergeTree(string $zkPath, string $replicaName): self
    {
        return $this->engine('ReplicatedMergeTree', $zkPath, $replicaName);
    }

    /**
     * Use ReplicatedReplacingMergeTree engine.
     */
    public function replicatedReplacingMergeTree(
        string $zkPath,
        string $replicaName,
        ?string $versionColumn = null
    ): self {
        $params = [$zkPath, $replicaName];
        if ($versionColumn) {
            $params[] = $versionColumn;
        }

        return $this->engine('ReplicatedReplacingMergeTree', ...$params);
    }

    /**
     * Use Distributed engine.
     */
    public function distributed(string $cluster, string $database, string $table, ?string $shardingKey = null): self
    {
        $params = [$cluster, $database, $table];
        if ($shardingKey) {
            $params[] = new Raw($shardingKey);
        }

        return $this->engine('Distributed', ...$params);
    }

    /**
     * Use Memory engine.
     */
    public function memory(): self
    {
        return $this->engine('Memory');
    }

    /**
     * Use Log engine.
     */
    public function log(): self
    {
        return $this->engine('Log');
    }

    /**
     * Use TinyLog engine.
     */
    public function tinyLog(): self
    {
        return $this->engine('TinyLog');
    }

    /**
     * Use StripeLog engine.
     */
    public function stripeLog(): self
    {
        return $this->engine('StripeLog');
    }

    /**
     * Use Buffer engine.
     */
    public function buffer(
        string $database,
        string $table,
        int $numLayers,
        int $minTime,
        int $maxTime,
        int $minRows,
        int $maxRows,
        int $minBytes,
        int $maxBytes,
    ): self {
        return $this->engine(
            'Buffer',
            $database,
            $table,
            $numLayers,
            $minTime,
            $maxTime,
            $minRows,
            $maxRows,
            $minBytes,
            $maxBytes
        );
    }

    /**
     * Use Null engine.
     */
    public function null(): self
    {
        return $this->engine('Null');
    }

    /**
     * Use Set engine.
     */
    public function set(): self
    {
        return $this->engine('Set');
    }

    /**
     * Use Join engine.
     */
    public function join(string $strictness, string $type, array $keys): self
    {
        return $this->engine('Join', $strictness, $type, ...$keys);
    }

    /**
     * Use URL engine.
     */
    public function url(string $url, string $format): self
    {
        return $this->engine('URL', $url, $format);
    }

    /**
     * Use File engine.
     */
    public function file(string $format): self
    {
        return $this->engine('File', $format);
    }

    /**
     * Use Merge engine.
     */
    public function merge(string $database, string $tableRegexp): self
    {
        return $this->engine('Merge', $database, $tableRegexp);
    }

    /**
     * Use Dictionary engine.
     */
    public function dictionary(string $dictionaryName): self
    {
        return $this->engine('Dictionary', $dictionaryName);
    }

    // ==================== TABLE STRUCTURE ====================

    /**
     * Set the PARTITION BY expression.
     */
    public function partitionBy(string $expression): self
    {
        $this->partitionBy = $expression;

        return $this;
    }

    /**
     * Set the ORDER BY columns.
     */
    public function orderBy(string|array $columns): self
    {
        $this->orderBy = is_array($columns) ? $columns : [$columns];

        return $this;
    }

    /**
     * Set the PRIMARY KEY columns.
     */
    public function primaryKey(string|array $columns): self
    {
        $this->primaryKey = is_array($columns) ? $columns : [$columns];

        return $this;
    }

    /**
     * Set the SAMPLE BY expression.
     */
    public function sampleBy(string $expression): self
    {
        $this->sampleBy = $expression;

        return $this;
    }

    /**
     * Set the TTL expression.
     */
    public function ttl(string $expression): self
    {
        $this->ttl = $expression;

        return $this;
    }

    /**
     * Set table settings.
     */
    public function settings(array $settings): self
    {
        $this->settings = array_merge($this->settings, $settings);

        return $this;
    }

    /**
     * Create table from SELECT query.
     */
    public function asSelect(string $query): self
    {
        $this->asSelect = $query;

        return $this;
    }

    /**
     * Create a temporary table.
     */
    public function temporary(bool $temporary = true): self
    {
        $this->temporary = $temporary;

        return $this;
    }

    // ==================== ALTER OPERATIONS ====================

    /**
     * Add a column (for ALTER).
     */
    public function addColumn(string $name, string $type): Column
    {
        $column = new Column($name, $type);
        $this->commands[] = ['type' => 'addColumn', 'column' => $column];

        return $column;
    }

    /**
     * Drop a column (for ALTER).
     */
    public function dropColumn(string $name): self
    {
        $this->commands[] = ['type' => 'dropColumn', 'name' => $name];

        return $this;
    }

    /**
     * Modify a column (for ALTER).
     */
    public function modifyColumn(string $name, string $type): Column
    {
        $column = new Column($name, $type);
        $this->commands[] = ['type' => 'modifyColumn', 'column' => $column];

        return $column;
    }

    /**
     * Rename a column (for ALTER).
     */
    public function renameColumn(string $from, string $to): self
    {
        $this->commands[] = ['type' => 'renameColumn', 'from' => $from, 'to' => $to];

        return $this;
    }

    /**
     * Clear a column (for ALTER).
     */
    public function clearColumn(string $name, ?string $partition = null): self
    {
        $this->commands[] = ['type' => 'clearColumn', 'name' => $name, 'partition' => $partition];

        return $this;
    }

    /**
     * Comment a column (for ALTER).
     */
    public function commentColumn(string $name, string $comment): self
    {
        $this->commands[] = ['type' => 'commentColumn', 'name' => $name, 'comment' => $comment];

        return $this;
    }

    /**
     * Add an index (for ALTER).
     */
    public function addIndex(string $name, string $expression, string $type, int $granularity): self
    {
        $this->commands[] = [
            'type' => 'addIndex',
            'name' => $name,
            'expression' => $expression,
            'indexType' => $type,
            'granularity' => $granularity,
        ];

        return $this;
    }

    /**
     * Drop an index (for ALTER).
     */
    public function dropIndex(string $name): self
    {
        $this->commands[] = ['type' => 'dropIndex', 'name' => $name];

        return $this;
    }

    /**
     * Materialize an index (for ALTER).
     */
    public function materializeIndex(string $name, ?string $partition = null): self
    {
        $this->commands[] = ['type' => 'materializeIndex', 'name' => $name, 'partition' => $partition];

        return $this;
    }

    /**
     * Add a projection (for ALTER).
     */
    public function addProjection(string $name, string $query): self
    {
        $this->commands[] = ['type' => 'addProjection', 'name' => $name, 'query' => $query];

        return $this;
    }

    /**
     * Drop a projection (for ALTER).
     */
    public function dropProjection(string $name): self
    {
        $this->commands[] = ['type' => 'dropProjection', 'name' => $name];

        return $this;
    }

    /**
     * Materialize a projection (for ALTER).
     */
    public function materializeProjection(string $name, ?string $partition = null): self
    {
        $this->commands[] = ['type' => 'materializeProjection', 'name' => $name, 'partition' => $partition];

        return $this;
    }

    /**
     * Modify TTL (for ALTER).
     */
    public function modifyTTL(string $expression): self
    {
        $this->commands[] = ['type' => 'modifyTTL', 'expression' => $expression];

        return $this;
    }

    /**
     * Remove TTL (for ALTER).
     */
    public function removeTTL(): self
    {
        $this->commands[] = ['type' => 'removeTTL'];

        return $this;
    }

    /**
     * Modify ORDER BY (for ALTER).
     */
    public function modifyOrderBy(string|array $columns): self
    {
        $this->commands[] = ['type' => 'modifyOrderBy', 'columns' => is_array($columns) ? $columns : [$columns]];

        return $this;
    }

    /**
     * Modify a setting (for ALTER).
     */
    public function modifySetting(string $name, mixed $value): self
    {
        $this->commands[] = ['type' => 'modifySetting', 'name' => $name, 'value' => $value];

        return $this;
    }

    /**
     * Reset a setting (for ALTER).
     */
    public function resetSetting(string $name): self
    {
        $this->commands[] = ['type' => 'resetSetting', 'name' => $name];

        return $this;
    }

    /**
     * Delete rows (for ALTER).
     */
    public function delete(string $where): self
    {
        $this->commands[] = ['type' => 'delete', 'where' => $where];

        return $this;
    }

    /**
     * Update rows (for ALTER).
     */
    public function update(array $assignments, string $where): self
    {
        $this->commands[] = ['type' => 'update', 'assignments' => $assignments, 'where' => $where];

        return $this;
    }

    // ==================== COLUMN TYPES ====================

    /**
     * Add a column to the table.
     */
    protected function addColumnDefinition(string $name, string $type): Column
    {
        $column = new Column($name, $type);
        $this->columns[] = $column;

        return $column;
    }

    /**
     * Format enum values for SQL.
     */
    protected function formatEnumValues(array $values): string
    {
        $parts = [];

        foreach ($values as $key => $value) {
            if (is_int($key)) {
                $parts[] = "'{$value}' = {$key}";
            } else {
                $parts[] = "'{$key}' = {$value}";
            }
        }

        return implode(', ', $parts);
    }
}
