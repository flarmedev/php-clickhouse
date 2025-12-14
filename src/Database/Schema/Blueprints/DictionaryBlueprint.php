<?php

declare(strict_types=1);

namespace Flarme\PhpClickhouse\Database\Schema\Blueprints;

use Flarme\PhpClickhouse\Database\Schema\Components\Attribute;

class DictionaryBlueprint extends Blueprint
{
    protected array $primaryKey = [];

    /** @var Attribute[] */
    protected array $attributes = [];

    protected ?array $source = null;

    protected ?array $layout = null;

    protected ?array $lifetime = null;

    protected ?array $range = null;

    // ==================== GETTERS ====================

    public function getPrimaryKey(): array
    {
        return $this->primaryKey;
    }

    /** @return Attribute[] */
    public function getAttributes(): array
    {
        return $this->attributes;
    }

    public function getSource(): ?array
    {
        return $this->source;
    }

    public function getLayout(): ?array
    {
        return $this->layout;
    }

    public function getLifetime(): ?array
    {
        return $this->lifetime;
    }

    public function getRange(): ?array
    {
        return $this->range;
    }

    // ==================== PRIMARY KEY ====================

    /**
     * Set the primary key columns.
     */
    public function primaryKey(string|array $columns): self
    {
        $this->primaryKey = is_array($columns) ? $columns : [$columns];

        return $this;
    }

    // ==================== ATTRIBUTES ====================

    /**
     * Add an attribute.
     */
    public function attribute(string $name, string $type, mixed $default = null): Attribute
    {
        $attribute = func_num_args() >= 3
            ? new Attribute($name, $type, $default)
            : new Attribute($name, $type);

        $this->attributes[] = $attribute;

        return $attribute;
    }

    /**
     * Add a hierarchical attribute.
     */
    public function hierarchicalAttribute(string $name, string $type, mixed $default = null): Attribute
    {
        return $this->attribute($name, $type, $default)->hierarchical();
    }

    /**
     * Add an injective attribute.
     */
    public function injectiveAttribute(string $name, string $type, mixed $default = null): Attribute
    {
        return $this->attribute($name, $type, $default)->injective();
    }

    /**
     * Add an expression attribute.
     */
    public function expressionAttribute(string $name, string $type, string $expression): Attribute
    {
        $attribute = new Attribute($name, $type);
        $attribute->expression($expression);
        $this->attributes[] = $attribute;

        return $attribute;
    }

    // ==================== SOURCES ====================

    /**
     * Set ClickHouse as the source.
     */
    public function sourceClickHouse(string $table, ?string $database = null, array $options = []): self
    {
        $this->source = [
            'type' => 'clickhouse',
            'table' => $table,
            'database' => $database,
            'options' => $options,
        ];

        return $this;
    }

    /**
     * Set MySQL as the source.
     */
    public function sourceMySQL(
        string $host,
        int $port,
        string $user,
        string $password,
        string $database,
        string $table,
        array $options = [],
    ): self {
        $this->source = [
            'type' => 'mysql',
            'host' => $host,
            'port' => $port,
            'user' => $user,
            'password' => $password,
            'database' => $database,
            'table' => $table,
            'options' => $options,
        ];

        return $this;
    }

    /**
     * Set PostgreSQL as the source.
     */
    public function sourcePostgreSQL(
        string $host,
        int $port,
        string $user,
        string $password,
        string $database,
        string $table,
        ?string $schema = null,
        array $options = [],
    ): self {
        $this->source = [
            'type' => 'postgresql',
            'host' => $host,
            'port' => $port,
            'user' => $user,
            'password' => $password,
            'database' => $database,
            'table' => $table,
            'schema' => $schema,
            'options' => $options,
        ];

        return $this;
    }

    /**
     * Set MongoDB as the source.
     */
    public function sourceMongoDB(
        string $host,
        int $port,
        string $user,
        string $password,
        string $database,
        string $collection,
        array $options = [],
    ): self {
        $this->source = [
            'type' => 'mongodb',
            'host' => $host,
            'port' => $port,
            'user' => $user,
            'password' => $password,
            'database' => $database,
            'collection' => $collection,
            'options' => $options,
        ];

        return $this;
    }

    /**
     * Set HTTP as the source.
     */
    public function sourceHTTP(string $url, string $format, array $options = []): self
    {
        $this->source = [
            'type' => 'http',
            'url' => $url,
            'format' => $format,
            'options' => $options,
        ];

        return $this;
    }

    /**
     * Set File as the source.
     */
    public function sourceFile(string $path, string $format, array $options = []): self
    {
        $this->source = [
            'type' => 'file',
            'path' => $path,
            'format' => $format,
            'options' => $options,
        ];

        return $this;
    }

    /**
     * Set Executable as the source.
     */
    public function sourceExecutable(string $command, string $format, array $options = []): self
    {
        $this->source = [
            'type' => 'executable',
            'command' => $command,
            'format' => $format,
            'options' => $options,
        ];

        return $this;
    }

    /**
     * Set ExecutablePool as the source.
     */
    public function sourceExecutablePool(string $command, string $format, int $size, array $options = []): self
    {
        $this->source = [
            'type' => 'executable_pool',
            'command' => $command,
            'format' => $format,
            'size' => $size,
            'options' => $options,
        ];

        return $this;
    }

    /**
     * Set Redis as the source.
     */
    public function sourceRedis(string $host, int $port, int $dbIndex = 0, ?string $password = null, array $options = []): self
    {
        $this->source = [
            'type' => 'redis',
            'host' => $host,
            'port' => $port,
            'db_index' => $dbIndex,
            'password' => $password,
            'options' => $options,
        ];

        return $this;
    }

    // ==================== LAYOUTS ====================

    /**
     * Use flat layout.
     */
    public function layoutFlat(int $initialArraySize = 1024, int $maxArraySize = 500000): self
    {
        $this->layout = [
            'type' => 'flat',
            'initial_array_size' => $initialArraySize,
            'max_array_size' => $maxArraySize,
        ];

        return $this;
    }

    /**
     * Use hashed layout.
     */
    public function layoutHashed(int $shards = 1): self
    {
        $this->layout = [
            'type' => 'hashed',
            'shards' => $shards,
        ];

        return $this;
    }

    /**
     * Use sparse_hashed layout.
     */
    public function layoutSparseHashed(int $shards = 1): self
    {
        $this->layout = [
            'type' => 'sparse_hashed',
            'shards' => $shards,
        ];

        return $this;
    }

    /**
     * Use hashed_array layout.
     */
    public function layoutHashedArray(int $shards = 1): self
    {
        $this->layout = [
            'type' => 'hashed_array',
            'shards' => $shards,
        ];

        return $this;
    }

    /**
     * Use complex_key_hashed layout.
     */
    public function layoutComplexKeyHashed(int $shards = 1): self
    {
        $this->layout = [
            'type' => 'complex_key_hashed',
            'shards' => $shards,
        ];

        return $this;
    }

    /**
     * Use complex_key_sparse_hashed layout.
     */
    public function layoutComplexKeySparseHashed(int $shards = 1): self
    {
        $this->layout = [
            'type' => 'complex_key_sparse_hashed',
            'shards' => $shards,
        ];

        return $this;
    }

    /**
     * Use range_hashed layout.
     */
    public function layoutRangeHashed(): self
    {
        $this->layout = ['type' => 'range_hashed'];

        return $this;
    }

    /**
     * Use complex_key_range_hashed layout.
     */
    public function layoutComplexKeyRangeHashed(): self
    {
        $this->layout = ['type' => 'complex_key_range_hashed'];

        return $this;
    }

    /**
     * Use cache layout.
     */
    public function layoutCache(int $sizeInCells): self
    {
        $this->layout = [
            'type' => 'cache',
            'size_in_cells' => $sizeInCells,
        ];

        return $this;
    }

    /**
     * Use complex_key_cache layout.
     */
    public function layoutComplexKeyCache(int $sizeInCells): self
    {
        $this->layout = [
            'type' => 'complex_key_cache',
            'size_in_cells' => $sizeInCells,
        ];

        return $this;
    }

    /**
     * Use ssd_cache layout.
     */
    public function layoutSSDCache(
        string $path,
        int $blockSize,
        int $fileSize,
        int $readBufferSize,
        int $writeBufferSize,
    ): self {
        $this->layout = [
            'type' => 'ssd_cache',
            'path' => $path,
            'block_size' => $blockSize,
            'file_size' => $fileSize,
            'read_buffer_size' => $readBufferSize,
            'write_buffer_size' => $writeBufferSize,
        ];

        return $this;
    }

    /**
     * Use complex_key_ssd_cache layout.
     */
    public function layoutComplexKeySSDCache(
        string $path,
        int $blockSize,
        int $fileSize,
        int $readBufferSize,
        int $writeBufferSize,
    ): self {
        $this->layout = [
            'type' => 'complex_key_ssd_cache',
            'path' => $path,
            'block_size' => $blockSize,
            'file_size' => $fileSize,
            'read_buffer_size' => $readBufferSize,
            'write_buffer_size' => $writeBufferSize,
        ];

        return $this;
    }

    /**
     * Use direct layout.
     */
    public function layoutDirect(): self
    {
        $this->layout = ['type' => 'direct'];

        return $this;
    }

    /**
     * Use complex_key_direct layout.
     */
    public function layoutComplexKeyDirect(): self
    {
        $this->layout = ['type' => 'complex_key_direct'];

        return $this;
    }

    /**
     * Use ip_trie layout.
     */
    public function layoutIPTrie(): self
    {
        $this->layout = ['type' => 'ip_trie'];

        return $this;
    }

    // ==================== LIFETIME ====================

    /**
     * Set the dictionary lifetime.
     */
    public function lifetime(int $min, ?int $max = null): self
    {
        $this->lifetime = [
            'min' => $min,
            'max' => $max ?? $min,
        ];

        return $this;
    }

    // ==================== RANGE ====================

    /**
     * Set the range columns (for range_hashed layout).
     */
    public function range(string $minColumn, string $maxColumn): self
    {
        $this->range = [
            'min' => $minColumn,
            'max' => $maxColumn,
        ];

        return $this;
    }
}
