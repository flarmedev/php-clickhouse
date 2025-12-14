<?php

declare(strict_types=1);

namespace Flarme\PhpClickhouse\Database\Schema\Components;

class Attribute
{
    protected string $name;

    protected string $type;

    protected mixed $default = null;

    protected bool $hasDefault = false;

    protected bool $hierarchical = false;

    protected bool $injective = false;

    protected bool $isExpression = false;

    protected ?string $expression = null;

    public function __construct(string $name, string $type, mixed $default = null)
    {
        $this->name = $name;
        $this->type = $type;

        if (func_num_args() >= 3) {
            $this->default = $default;
            $this->hasDefault = true;
        }
    }

    public function getName(): string
    {
        return $this->name;
    }

    public function getType(): string
    {
        return $this->type;
    }

    public function getDefault(): mixed
    {
        return $this->default;
    }

    public function hasDefault(): bool
    {
        return $this->hasDefault;
    }

    public function isHierarchical(): bool
    {
        return $this->hierarchical;
    }

    public function isInjective(): bool
    {
        return $this->injective;
    }

    public function isExpression(): bool
    {
        return $this->isExpression;
    }

    public function getExpression(): ?string
    {
        return $this->expression;
    }

    /**
     * Mark the attribute as hierarchical.
     */
    public function hierarchical(bool $hierarchical = true): self
    {
        $this->hierarchical = $hierarchical;

        return $this;
    }

    /**
     * Mark the attribute as injective.
     */
    public function injective(bool $injective = true): self
    {
        $this->injective = $injective;

        return $this;
    }

    /**
     * Set the attribute as an expression.
     */
    public function expression(string $expression): self
    {
        $this->isExpression = true;
        $this->expression = $expression;

        return $this;
    }
}
