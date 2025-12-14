<?php

declare(strict_types=1);

namespace Flarme\PhpClickhouse\Database\Schema\Blueprints;

use Flarme\PhpClickhouse\Database\Schema\Components\Column;

class ViewBlueprint extends Blueprint
{
    protected bool $materialized = false;

    protected ?string $toTable = null;

    protected ?string $toDatabase = null;

    protected ?string $engine = null;

    protected array $engineParams = [];

    protected ?string $partitionBy = null;

    protected array $orderBy = [];

    protected array $primaryKey = [];

    protected ?string $ttl = null;

    protected array $settings = [];

    protected ?string $asSelect = null;

    protected bool $populate = false;

    protected bool $refreshable = false;

    protected ?string $refreshInterval = null;

    /** @var Column[] */
    protected array $columns = [];

    // ==================== GETTERS ====================

    public function isMaterialized(): bool
    {
        return $this->materialized;
    }

    public function getToTable(): ?string
    {
        return $this->toTable;
    }

    public function getToDatabase(): ?string
    {
        return $this->toDatabase;
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

    public function shouldPopulate(): bool
    {
        return $this->populate;
    }

    public function isRefreshable(): bool
    {
        return $this->refreshable;
    }

    public function getRefreshInterval(): ?string
    {
        return $this->refreshInterval;
    }

    /** @return Column[] */
    public function getColumns(): array
    {
        return $this->columns;
    }

    // ==================== VIEW CONFIGURATION ====================

    /**
     * Make this a materialized view.
     */
    public function materialized(bool $materialized = true): self
    {
        $this->materialized = $materialized;

        return $this;
    }

    /**
     * Set the target table for materialized view.
     */
    public function to(string $table, ?string $database = null): self
    {
        $this->toTable = $table;
        $this->toDatabase = $database;

        return $this;
    }

    /**
     * Set the SELECT query for the view.
     */
    public function as(string $selectQuery): self
    {
        $this->asSelect = $selectQuery;

        return $this;
    }

    /**
     * Populate the materialized view with existing data.
     */
    public function populate(bool $populate = true): self
    {
        $this->populate = $populate;

        return $this;
    }

    /**
     * Make this a refreshable materialized view.
     */
    public function refreshable(string $interval): self
    {
        $this->refreshable = true;
        $this->refreshInterval = $interval;

        return $this;
    }

    // ==================== ENGINE CONFIGURATION (for materialized views) ====================

    /**
     * Set the storage engine.
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
    public function replacingMergeTree(?string $versionColumn = null): self
    {
        return $versionColumn
            ? $this->engine('ReplacingMergeTree', $versionColumn)
            : $this->engine('ReplacingMergeTree');
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

    // ==================== TABLE STRUCTURE (for materialized views) ====================

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

    // ==================== COLUMN DEFINITIONS (optional) ====================

    /**
     * Add a column definition.
     */
    public function column(string $name, string $type): Column
    {
        $column = new Column($name, $type);
        $this->columns[] = $column;

        return $column;
    }
}
