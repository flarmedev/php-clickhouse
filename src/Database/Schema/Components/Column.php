<?php

declare(strict_types=1);

namespace Flarme\PhpClickhouse\Database\Schema\Components;

class Column
{
    protected string $name;

    protected string $type;

    protected bool $nullable = false;

    protected mixed $default = null;

    protected bool $hasDefault = false;

    protected ?string $defaultExpression = null;

    protected ?string $materialized = null;

    protected ?string $alias = null;

    protected ?string $codec = null;

    protected ?string $ttl = null;

    protected ?string $comment = null;

    protected ?string $after = null;

    protected bool $first = false;

    public function __construct(string $name, string $type)
    {
        $this->name = $name;
        $this->type = $type;
    }

    public function getName(): string
    {
        return $this->name;
    }

    public function getType(): string
    {
        return $this->type;
    }

    public function isNullable(): bool
    {
        return $this->nullable;
    }

    public function getDefault(): mixed
    {
        return $this->default;
    }

    public function hasDefault(): bool
    {
        return $this->hasDefault;
    }

    public function getDefaultExpression(): ?string
    {
        return $this->defaultExpression;
    }

    public function getMaterialized(): ?string
    {
        return $this->materialized;
    }

    public function getAlias(): ?string
    {
        return $this->alias;
    }

    public function getCodec(): ?string
    {
        return $this->codec;
    }

    public function getTtl(): ?string
    {
        return $this->ttl;
    }

    public function getComment(): ?string
    {
        return $this->comment;
    }

    public function getAfter(): ?string
    {
        return $this->after;
    }

    public function isFirst(): bool
    {
        return $this->first;
    }

    /**
     * Make the column nullable.
     */
    public function nullable(bool $nullable = true): self
    {
        $this->nullable = $nullable;

        return $this;
    }

    /**
     * Set a default value for the column.
     */
    public function default(mixed $value): self
    {
        $this->default = $value;
        $this->hasDefault = true;

        return $this;
    }

    /**
     * Set a default expression for the column.
     */
    public function defaultExpression(string $expression): self
    {
        $this->defaultExpression = $expression;

        return $this;
    }

    /**
     * Set a materialized expression for the column.
     */
    public function materialized(string $expression): self
    {
        $this->materialized = $expression;

        return $this;
    }

    /**
     * Set an alias expression for the column.
     */
    public function alias(string $expression): self
    {
        $this->alias = $expression;

        return $this;
    }

    /**
     * Set the compression codec for the column.
     */
    public function codec(string $codec): self
    {
        $this->codec = $codec;

        return $this;
    }

    /**
     * Set the TTL expression for the column.
     */
    public function ttl(string $expression): self
    {
        $this->ttl = $expression;

        return $this;
    }

    /**
     * Set a comment for the column.
     */
    public function comment(string $comment): self
    {
        $this->comment = $comment;

        return $this;
    }

    /**
     * Position the column after another column (for ALTER).
     */
    public function after(string $column): self
    {
        $this->after = $column;
        $this->first = false;

        return $this;
    }

    /**
     * Position the column first (for ALTER).
     */
    public function first(): self
    {
        $this->first = true;
        $this->after = null;

        return $this;
    }
}
