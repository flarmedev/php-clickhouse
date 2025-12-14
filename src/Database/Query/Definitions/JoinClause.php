<?php

declare(strict_types=1);

namespace Flarme\PhpClickhouse\Database\Query\Definitions;

use Closure;
use Flarme\PhpClickhouse\Database\Query\Builder;
use Flarme\PhpClickhouse\Expressions\Raw;

class JoinClause
{
    public string $type;

    public string|Raw $table;

    public ?string $alias = null;

    public array $clauses = [];

    public ?array $using = null;

    protected Builder $parentQuery;

    public function __construct(Builder $parentQuery, string $type, string|Raw $table)
    {
        $this->parentQuery = $parentQuery;
        $this->type = $type;
        $this->table = $table;
    }

    public function from(string|Raw $table): static
    {
        $this->table = $table;

        return $this;
    }

    public function as(string $alias): static
    {
        $this->alias = $alias;

        return $this;
    }

    public function on(
        Closure|string|Raw $first,
        ?string $operator = null,
        mixed $second = null,
        string $boolean = 'AND'
    ): static {
        if ($first instanceof Closure) {
            return $this->onNested($first, $boolean);
        }

        $this->clauses[] = [
            'type' => 'column',
            'first' => $first,
            'operator' => $operator,
            'second' => $second,
            'boolean' => $boolean,
        ];

        return $this;
    }

    public function orOn(Closure|string|Raw $first, ?string $operator = null, mixed $second = null): static
    {
        return $this->on($first, $operator, $second, 'OR');
    }

    public function using(string|Raw ...$columns): static
    {
        $this->using = $columns;

        return $this;
    }

    public function global(): static
    {
        if ( ! str_starts_with($this->type, 'GLOBAL ')) {
            $this->type = 'GLOBAL ' . $this->type;
        }

        return $this;
    }

    public function any(): static
    {
        return $this->addStrictness('ANY');
    }

    public function all(): static
    {
        return $this->addStrictness('ALL');
    }

    public function semi(): static
    {
        return $this->addStrictness('SEMI');
    }

    public function anti(): static
    {
        return $this->addStrictness('ANTI');
    }

    public function asof(): static
    {
        return $this->addStrictness('ASOF');
    }

    protected function onNested(Closure $callback, string $boolean = 'AND'): static
    {
        $join = new static($this->parentQuery, $this->type, $this->table);
        $callback($join);

        if (count($join->clauses) > 0) {
            $this->clauses[] = [
                'type' => 'nested',
                'join' => $join,
                'boolean' => $boolean,
            ];
        }

        return $this;
    }

    protected function addStrictness(string $strictness): static
    {
        $parts = explode(' ', $this->type);
        $direction = array_pop($parts);

        if (in_array($direction, ['INNER', 'LEFT', 'RIGHT', 'FULL', 'CROSS'])) {
            $parts[] = $strictness;
            $parts[] = $direction;
        } else {
            $parts[] = $direction;
            $parts[] = $strictness;
        }

        $this->type = implode(' ', array_filter($parts));

        return $this;
    }
}
