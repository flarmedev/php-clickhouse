<?php

declare(strict_types=1);

namespace Flarme\PhpClickhouse\Database\Query;

use Closure;
use Flarme\PhpClickhouse\Contracts\ClientInterface;
use Flarme\PhpClickhouse\Contracts\ResponseInterface;
use Flarme\PhpClickhouse\Database\Exceptions\MissingClientException;
use Flarme\PhpClickhouse\Database\Query\Definitions\JoinClause;
use Flarme\PhpClickhouse\Database\Query\Definitions\WindowClause;
use Flarme\PhpClickhouse\Exceptions\UnsupportedBindingException;
use Flarme\PhpClickhouse\Expressions\Raw;
use Flarme\PhpClickhouse\Query;
use Generator;

class Builder
{
    /**
     * Valid SQL comparison operators.
     */
    public const OPERATORS = [
        '=', '!=', '<>', '<', '>', '<=', '>=',
        'LIKE', 'NOT LIKE', 'ILIKE', 'NOT ILIKE',
        'IN', 'NOT IN', 'BETWEEN', 'NOT BETWEEN',
    ];

    /**
     * Valid SQL comparison operators for column comparisons.
     */
    public const COLUMN_OPERATORS = [
        '=', '!=', '<>', '<', '>', '<=', '>=',
    ];

    public ?ClientInterface $client = null;

    public Grammar $grammar;

    public array $bindings = [];

    public ?array $columns = null;

    public bool $distinct = false;

    public string|Raw|null $from = null;

    public ?string $fromAlias = null;

    public bool $final = false;

    public ?array $sample = null;

    public array $joins = [];

    public array $arrayJoins = [];

    public array $wheres = [];

    public array $preWheres = [];

    public ?array $groups = null;

    public ?array $groupModifiers = null;

    public array $havings = [];

    public array $orders = [];

    public ?int $limit = null;

    public ?int $offset = null;

    public ?array $limitBy = null;

    public array $unions = [];

    public array $intersects = [];

    public array $ctes = [];

    public array $windows = [];

    public array $qualifies = [];

    public function __construct(?ClientInterface $client = null)
    {
        $this->client = $client;
        $this->grammar = new Grammar();
    }

    public static function query(?ClientInterface $client = null): self
    {
        return new self($client);
    }

    public function newQuery(): self
    {
        return static::query($this->client);
    }

    public function forSubQuery(): static
    {
        return $this->newQuery();
    }

    // ==================== WITH (CTE) ====================

    public function with(string $name, Closure|self $query): static
    {
        $subQuery = $this->createSub($query);

        $this->ctes[] = [
            'name' => $name,
            'query' => $subQuery,
        ];

        return $this;
    }

    // ==================== SELECT ====================

    public function select(mixed $columns = ['*']): static
    {
        $this->columns = [];

        $columns = is_array($columns) ? $columns : func_get_args();

        foreach ($columns as $column) {
            $this->columns[] = $column;
        }

        return $this;
    }

    public function addSelect(mixed $column): static
    {
        $columns = is_array($column) ? $column : func_get_args();

        foreach ($columns as $col) {
            $this->columns[] = $col;
        }

        return $this;
    }

    public function selectRaw(string $expression, array $bindings = []): static
    {
        $this->columns[] = new Raw($expression);
        $this->addBinding($bindings, 'select');

        return $this;
    }

    public function distinct(): static
    {
        $this->distinct = true;

        return $this;
    }

    // ==================== FROM ====================

    public function from(string|Raw|Closure $table, ?string $as = null): static
    {
        if ($table instanceof Closure) {
            $this->from = new Raw('(' . $this->createSub($table) . ')');
        } else {
            $this->from = $table;
        }

        $this->fromAlias = $as;

        return $this;
    }

    public function fromSub(Closure|self $query, string $as): static
    {
        $this->from = new Raw('(' . $this->createSub($query) . ')');
        $this->fromAlias = $as;

        return $this;
    }

    public function as(string $alias): static
    {
        $this->fromAlias = $alias;

        return $this;
    }

    public function final(bool $final = true): static
    {
        $this->final = $final;

        return $this;
    }

    public function sample(int|float $sample, int|float|null $offset = null): static
    {
        $this->sample = compact('sample', 'offset');

        return $this;
    }

    // ==================== JOIN ====================

    public function join(
        string|Raw $table,
        Closure|string|null $first = null,
        ?string $operator = null,
        mixed $second = null,
        string $type = 'INNER'
    ): static {
        $join = $this->newJoinClause($type, $table);

        if ($first instanceof Closure) {
            $first($join);
        } elseif ($first !== null) {
            $join->on($first, $operator, $second);
        }

        $this->joins[] = $join;

        return $this;
    }

    public function joinSub(
        Closure|self $query,
        string $as,
        Closure|string $first,
        ?string $operator = null,
        mixed $second = null,
        string $type = 'INNER'
    ): static {
        $subQuery = $this->createSub($query);

        return $this->join(new Raw("({$subQuery}) AS `{$as}`"), $first, $operator, $second, $type);
    }

    public function leftJoin(
        string|Raw $table,
        Closure|string|null $first = null,
        ?string $operator = null,
        mixed $second = null
    ): static {
        return $this->join($table, $first, $operator, $second, 'LEFT');
    }

    public function leftJoinSub(
        Closure|self $query,
        string $as,
        Closure|string $first,
        ?string $operator = null,
        mixed $second = null
    ): static {
        return $this->joinSub($query, $as, $first, $operator, $second, 'LEFT');
    }

    public function rightJoin(
        string|Raw $table,
        Closure|string|null $first = null,
        ?string $operator = null,
        mixed $second = null
    ): static {
        return $this->join($table, $first, $operator, $second, 'RIGHT');
    }

    public function crossJoin(string|Raw $table): static
    {
        return $this->join($table, null, null, null, 'CROSS');
    }

    public function asofJoin(
        string|Raw $table,
        Closure|string|null $first = null,
        ?string $operator = null,
        mixed $second = null
    ): static {
        return $this->join($table, $first, $operator, $second, 'ASOF');
    }

    public function leftAsofJoin(
        string|Raw $table,
        Closure|string|null $first = null,
        ?string $operator = null,
        mixed $second = null
    ): static {
        return $this->join($table, $first, $operator, $second, 'LEFT ASOF');
    }

    public function globalJoin(
        string|Raw $table,
        Closure|string|null $first = null,
        ?string $operator = null,
        mixed $second = null,
        string $type = 'INNER'
    ): static {
        return $this->join($table, $first, $operator, $second, "GLOBAL {$type}");
    }

    public function globalLeftJoin(
        string|Raw $table,
        Closure|string|null $first = null,
        ?string $operator = null,
        mixed $second = null
    ): static {
        return $this->globalJoin($table, $first, $operator, $second, 'LEFT');
    }

    // ==================== ARRAY JOIN ====================

    public function arrayJoin(string|Raw $column, ?string $alias = null): static
    {
        $this->arrayJoins[] = [
            'column' => $column,
            'alias' => $alias,
            'type' => 'ARRAY JOIN',
        ];

        return $this;
    }

    public function leftArrayJoin(string|Raw $column, ?string $alias = null): static
    {
        $this->arrayJoins[] = [
            'column' => $column,
            'alias' => $alias,
            'type' => 'LEFT ARRAY JOIN',
        ];

        return $this;
    }

    // ==================== WHERE ====================

    public function where(mixed $column, mixed $operator = null, mixed $value = null, string $boolean = 'AND'): static
    {
        // Handle closure for nested where
        if ($column instanceof Closure && $operator === null) {
            return $this->whereNested($column, $boolean);
        }

        // Handle 2-argument form: where('column', 'value') - swap operator and value
        if ($value === null && $operator !== null && ! $this->isOperator($operator)) {
            [$value, $operator] = [$operator, '='];
        }

        // Handle null value with = operator -> IS NULL
        if ($value === null && $operator === '=') {
            return $this->whereNull($column, $boolean, false);
        }

        // Handle null value with != or <> operator -> IS NOT NULL
        if ($value === null && in_array($operator, ['!=', '<>'])) {
            return $this->whereNull($column, $boolean, true);
        }

        $type = 'basic';
        $this->wheres[] = compact('type', 'column', 'operator', 'value', 'boolean');
        $this->addBinding($value, 'where');

        return $this;
    }

    public function orWhere(mixed $column, mixed $operator = null, mixed $value = null): static
    {
        // Handle 2-argument form
        if ($value === null && $operator !== null && ! $this->isOperator($operator)) {
            [$value, $operator] = [$operator, '='];
        }

        return $this->where($column, $operator, $value, 'OR');
    }

    public function whereRaw(string $sql, array $bindings = [], string $boolean = 'AND'): static
    {
        $this->wheres[] = [
            'type' => 'raw',
            'sql' => $sql,
            'boolean' => $boolean,
        ];

        $this->addBinding($bindings, 'where');

        return $this;
    }

    public function orWhereRaw(string $sql, array $bindings = []): static
    {
        return $this->whereRaw($sql, $bindings, 'OR');
    }

    public function whereNested(Closure $callback, string $boolean = 'AND'): static
    {
        $query = $this->forNestedWhere();
        $callback($query);

        if (count($query->wheres) > 0) {
            $this->wheres[] = [
                'type' => 'nested',
                'query' => $query,
                'boolean' => $boolean,
            ];

            $this->addBinding($query->bindings, 'where');
        }

        return $this;
    }

    public function forNestedWhere(): static
    {
        return $this->newQuery();
    }

    public function whereIn(
        string|Raw $column,
        array|Closure|self $values,
        string $boolean = 'AND',
        bool $not = false
    ): static {
        if ($values instanceof Closure || $values instanceof self) {
            $subQuery = $this->createSub($values);
            $this->wheres[] = [
                'type' => 'inSub',
                'column' => $column,
                'query' => $subQuery,
                'boolean' => $boolean,
                'not' => $not,
            ];
        } else {
            $this->wheres[] = [
                'type' => 'in',
                'column' => $column,
                'values' => $values,
                'boolean' => $boolean,
                'not' => $not,
            ];

            $this->addBinding($values, 'where');
        }

        return $this;
    }

    public function orWhereIn(string|Raw $column, array|Closure|self $values): static
    {
        return $this->whereIn($column, $values, 'OR');
    }

    public function whereNotIn(string|Raw $column, array|Closure|self $values, string $boolean = 'AND'): static
    {
        return $this->whereIn($column, $values, $boolean, true);
    }

    public function orWhereNotIn(string|Raw $column, array|Closure|self $values): static
    {
        return $this->whereNotIn($column, $values, 'OR');
    }

    public function whereGlobalIn(
        string|Raw $column,
        array|Closure|self $values,
        string $boolean = 'AND',
        bool $not = false
    ): static {
        if ($values instanceof Closure || $values instanceof self) {
            $subQuery = $this->createSub($values);
            $this->wheres[] = [
                'type' => 'globalInSub',
                'column' => $column,
                'query' => $subQuery,
                'boolean' => $boolean,
                'not' => $not,
            ];
        } else {
            $this->wheres[] = [
                'type' => 'globalIn',
                'column' => $column,
                'values' => $values,
                'boolean' => $boolean,
                'not' => $not,
            ];

            $this->addBinding($values, 'where');
        }

        return $this;
    }

    public function orWhereGlobalIn(string|Raw $column, array|Closure|self $values): static
    {
        return $this->whereGlobalIn($column, $values, 'OR');
    }

    public function whereGlobalNotIn(string|Raw $column, array|Closure|self $values, string $boolean = 'AND'): static
    {
        return $this->whereGlobalIn($column, $values, $boolean, true);
    }

    public function orWhereGlobalNotIn(string|Raw $column, array|Closure|self $values): static
    {
        return $this->whereGlobalNotIn($column, $values, 'OR');
    }

    public function whereBetween(string|Raw $column, array $values, string $boolean = 'AND', bool $not = false): static
    {
        $this->wheres[] = [
            'type' => 'between',
            'column' => $column,
            'values' => $values,
            'boolean' => $boolean,
            'not' => $not,
        ];

        $this->addBinding(array_slice($values, 0, 2), 'where');

        return $this;
    }

    public function orWhereBetween(string|Raw $column, array $values): static
    {
        return $this->whereBetween($column, $values, 'OR');
    }

    public function whereNotBetween(string|Raw $column, array $values, string $boolean = 'AND'): static
    {
        return $this->whereBetween($column, $values, $boolean, true);
    }

    public function orWhereNotBetween(string|Raw $column, array $values): static
    {
        return $this->whereNotBetween($column, $values, 'OR');
    }

    public function whereNull(string|Raw $column, string $boolean = 'AND', bool $not = false): static
    {
        $this->wheres[] = [
            'type' => 'null',
            'column' => $column,
            'boolean' => $boolean,
            'not' => $not,
        ];

        return $this;
    }

    public function orWhereNull(string|Raw $column): static
    {
        return $this->whereNull($column, 'OR');
    }

    public function whereNotNull(string|Raw $column, string $boolean = 'AND'): static
    {
        return $this->whereNull($column, $boolean, true);
    }

    public function orWhereNotNull(string|Raw $column): static
    {
        return $this->whereNotNull($column, 'OR');
    }

    public function whereColumn(
        string|Raw $first,
        ?string $operator = null,
        ?string $second = null,
        string $boolean = 'AND'
    ): static {
        // Handle 2-argument form: whereColumn('col1', 'col2')
        if ($second === null && $operator !== null && ! in_array(
            mb_strtoupper($operator),
            self::COLUMN_OPERATORS,
            true
        )) {
            [$second, $operator] = [$operator, '='];
        }

        $this->wheres[] = [
            'type' => 'column',
            'first' => $first,
            'operator' => $operator ?? '=',
            'second' => $second,
            'boolean' => $boolean,
        ];

        return $this;
    }

    public function orWhereColumn(string|Raw $first, ?string $operator = null, ?string $second = null): static
    {
        return $this->whereColumn($first, $operator, $second, 'OR');
    }

    public function whereExists(Closure|self $query, string $boolean = 'AND', bool $not = false): static
    {
        $subQuery = $this->createSub($query);

        $this->wheres[] = [
            'type' => 'exists',
            'query' => $subQuery,
            'boolean' => $boolean,
            'not' => $not,
        ];

        return $this;
    }

    public function orWhereExists(Closure|self $query): static
    {
        return $this->whereExists($query, 'OR');
    }

    public function whereNotExists(Closure|self $query, string $boolean = 'AND'): static
    {
        return $this->whereExists($query, $boolean, true);
    }

    public function orWhereNotExists(Closure|self $query): static
    {
        return $this->whereNotExists($query, 'OR');
    }

    public function whereLike(string|Raw $column, string $value, string $boolean = 'AND', bool $not = false): static
    {
        $operator = $not ? 'NOT LIKE' : 'LIKE';

        $this->wheres[] = [
            'column' => $column,
            'operator' => $operator,
            'value' => $value,
            'boolean' => $boolean,
        ];

        $this->addBinding($value, 'where');

        return $this;
    }

    public function orWhereLike(string|Raw $column, string $value): static
    {
        return $this->whereLike($column, $value, 'OR');
    }

    public function whereNotLike(string|Raw $column, string $value, string $boolean = 'AND'): static
    {
        return $this->whereLike($column, $value, $boolean, true);
    }

    // ==================== PREWHERE (ClickHouse-specific) ====================

    public function preWhere(
        mixed $column,
        mixed $operator = null,
        mixed $value = null,
        string $boolean = 'AND'
    ): static {
        if ($column instanceof Closure && $operator === null) {
            return $this->preWhereNested($column, $boolean);
        }

        // Handle 2-argument form
        if (func_num_args() === 2 || (func_num_args() >= 2 && $value === null && $operator !== null && ! in_array(
            mb_strtoupper((string) $operator),
            ['=', '!=', '<>', '<', '>', '<=', '>=', 'LIKE', 'NOT LIKE', 'ILIKE', 'IN', 'NOT IN']
        ))) {
            [$value, $operator] = [$operator, '='];
        }

        $this->preWheres[] = compact('column', 'operator', 'value', 'boolean');
        $this->addBinding($value, 'prewhere');

        return $this;
    }

    public function orPreWhere(mixed $column, mixed $operator = null, mixed $value = null): static
    {
        [$value, $operator] = $this->prepareValueAndOperator($value, $operator, func_num_args() === 2);

        return $this->preWhere($column, $operator, $value, 'OR');
    }

    public function preWhereRaw(string $sql, array $bindings = [], string $boolean = 'AND'): static
    {
        $this->preWheres[] = [
            'type' => 'raw',
            'sql' => $sql,
            'boolean' => $boolean,
        ];

        $this->addBinding($bindings, 'prewhere');

        return $this;
    }

    public function orPreWhereRaw(string $sql, array $bindings = []): static
    {
        return $this->preWhereRaw($sql, $bindings, 'OR');
    }

    // ==================== GROUP BY ====================

    public function groupBy(mixed ...$groups): static
    {
        foreach ($groups as $group) {
            $this->groups[] = $group;
        }

        return $this;
    }

    public function groupByRaw(string $sql, array $bindings = []): static
    {
        $this->groups[] = new Raw($sql);
        $this->addBinding($bindings, 'groupBy');

        return $this;
    }

    public function withRollup(): static
    {
        $this->groupModifiers['rollup'] = true;

        return $this;
    }

    public function withCube(): static
    {
        $this->groupModifiers['cube'] = true;

        return $this;
    }

    public function withTotals(): static
    {
        $this->groupModifiers['totals'] = true;

        return $this;
    }

    // ==================== HAVING ====================

    public function having(mixed $column, mixed $operator = null, mixed $value = null, string $boolean = 'AND'): static
    {
        if ($column instanceof Closure && $operator === null) {
            return $this->havingNested($column, $boolean);
        }

        // Handle 2-argument form
        if (func_num_args() === 2 || (func_num_args() >= 2 && $value === null && $operator !== null && ! in_array(
            mb_strtoupper((string) $operator),
            ['=', '!=', '<>', '<', '>', '<=', '>=', 'LIKE', 'NOT LIKE', 'ILIKE', 'IN', 'NOT IN']
        ))) {
            [$value, $operator] = [$operator, '='];
        }

        $this->havings[] = compact('column', 'operator', 'value', 'boolean');
        $this->addBinding($value, 'having');

        return $this;
    }

    public function orHaving(mixed $column, mixed $operator = null, mixed $value = null): static
    {
        [$value, $operator] = $this->prepareValueAndOperator($value, $operator, func_num_args() === 2);

        return $this->having($column, $operator, $value, 'OR');
    }

    public function havingRaw(string $sql, array $bindings = [], string $boolean = 'AND'): static
    {
        $this->havings[] = [
            'type' => 'raw',
            'sql' => $sql,
            'boolean' => $boolean,
        ];

        $this->addBinding($bindings, 'having');

        return $this;
    }

    public function orHavingRaw(string $sql, array $bindings = []): static
    {
        return $this->havingRaw($sql, $bindings, 'OR');
    }

    // ==================== WINDOW ====================

    public function window(string $name, Closure $callback): static
    {
        $window = new WindowClause();
        $callback($window);

        $this->windows[] = [
            'name' => $name,
            'definition' => $window,
        ];

        return $this;
    }

    // ==================== QUALIFY (ClickHouse-specific) ====================

    public function qualify(mixed $column, mixed $operator = null, mixed $value = null, string $boolean = 'AND'): static
    {
        // Handle 2-argument form
        if (func_num_args() === 2 || (func_num_args() >= 2 && $value === null && $operator !== null && ! in_array(
            mb_strtoupper((string) $operator),
            ['=', '!=', '<>', '<', '>', '<=', '>=', 'LIKE', 'NOT LIKE', 'ILIKE', 'IN', 'NOT IN']
        ))) {
            [$value, $operator] = [$operator, '='];
        }

        $this->qualifies[] = compact('column', 'operator', 'value', 'boolean');
        $this->addBinding($value, 'qualify');

        return $this;
    }

    public function orQualify(mixed $column, mixed $operator = null, mixed $value = null): static
    {
        [$value, $operator] = $this->prepareValueAndOperator($value, $operator, func_num_args() === 2);

        return $this->qualify($column, $operator, $value, 'OR');
    }

    public function qualifyRaw(string $sql, array $bindings = [], string $boolean = 'AND'): static
    {
        $this->qualifies[] = [
            'type' => 'raw',
            'sql' => $sql,
            'boolean' => $boolean,
        ];

        $this->addBinding($bindings, 'qualify');

        return $this;
    }

    // ==================== ORDER BY ====================

    public function orderBy(string|Raw $column, string $direction = 'ASC'): static
    {
        $this->orders[] = [
            'column' => $column,
            'direction' => mb_strtoupper($direction),
        ];

        return $this;
    }

    public function orderByDesc(string|Raw $column): static
    {
        return $this->orderBy($column, 'DESC');
    }

    public function orderByRaw(string $sql, array $bindings = []): static
    {
        $this->orders[] = [
            'type' => 'raw',
            'sql' => $sql,
        ];

        $this->addBinding($bindings, 'order');

        return $this;
    }

    public function latest(string $column = 'created_at'): static
    {
        return $this->orderByDesc($column);
    }

    public function oldest(string $column = 'created_at'): static
    {
        return $this->orderBy($column);
    }

    public function reorder(?string $column = null, string $direction = 'ASC'): static
    {
        $this->orders = [];

        if ($column !== null) {
            return $this->orderBy($column, $direction);
        }

        return $this;
    }

    // ==================== LIMIT / OFFSET ====================

    public function limit(int $limit): static
    {
        $this->limit = max(0, $limit);

        return $this;
    }

    public function take(int $value): static
    {
        return $this->limit($value);
    }

    public function offset(int $offset): static
    {
        $this->offset = max(0, $offset);

        return $this;
    }

    public function skip(int $value): static
    {
        return $this->offset($value);
    }

    public function forPage(int $page, int $perPage = 15): static
    {
        return $this->offset(($page - 1) * $perPage)->limit($perPage);
    }

    // ==================== LIMIT BY (ClickHouse-specific) ====================

    public function limitBy(int $limit, string|Raw ...$columns): static
    {
        $this->limitBy = [
            'limit' => $limit,
            'offset' => null,
            'columns' => $columns,
        ];

        return $this;
    }

    public function limitByOffset(int $limit, int $offset, string|Raw ...$columns): static
    {
        $this->limitBy = [
            'limit' => $limit,
            'offset' => $offset,
            'columns' => $columns,
        ];

        return $this;
    }

    // ==================== UNION / INTERSECT ====================

    public function union(Closure|self $query, bool $all = true): static
    {
        $this->unions[] = [
            'query' => $this->createSub($query),
            'all' => $all,
        ];

        return $this;
    }

    public function unionAll(Closure|self $query): static
    {
        return $this->union($query, true);
    }

    public function intersect(Closure|self $query, bool $distinct = false): static
    {
        $this->intersects[] = [
            'query' => $this->createSub($query),
            'distinct' => $distinct,
        ];

        return $this;
    }

    // ==================== CONDITIONAL BUILDING ====================

    public function when(mixed $value, callable $callback, ?callable $default = null): static
    {
        $value = $value instanceof Closure ? $value($this) : $value;

        if ($value) {
            return $callback($this, $value) ?? $this;
        }

        if ($default) {
            return $default($this, $value) ?? $this;
        }

        return $this;
    }

    public function unless(mixed $value, callable $callback, ?callable $default = null): static
    {
        $value = $value instanceof Closure ? $value($this) : $value;

        if ( ! $value) {
            return $callback($this, $value) ?? $this;
        }

        if ($default) {
            return $default($this, $value) ?? $this;
        }

        return $this;
    }

    // ==================== EXECUTION ====================

    public function toSql(): string
    {
        return $this->grammar->compileSelect($this);
    }

    /**
     * @throws UnsupportedBindingException
     */
    public function toRawSql(): string
    {
        return $this->toQuery()->toRawSql();
    }

    public function toQuery(): Query
    {
        return new Query($this->toSql(), $this->getBindings());
    }

    /**
     * @throws MissingClientException
     */
    public function execute(): ResponseInterface
    {
        if ( ! $this->client) {
            throw new MissingClientException();
        }

        return $this->client->execute($this->toQuery());
    }

    /**
     * @return array<int, array<string, mixed>>
     */
    public function get(): array
    {
        return $this->execute()->toArray();
    }

    public function cursor(): Generator
    {
        return $this->execute()->rows();
    }

    /**
     * @return array<string, mixed>|null
     */
    public function first(): ?array
    {
        $result = $this->limit(1)->execute()->first();

        return $result ?: null;
    }

    public function value(string $column): mixed
    {
        $result = $this->first();

        return $result[$column] ?? null;
    }

    /**
     * @return array<int|string, mixed>
     */
    public function pluck(string $column, ?string $key = null): array
    {
        $results = $this->get();

        if ($key === null) {
            return array_column($results, $column);
        }

        $plucked = [];
        foreach ($results as $row) {
            $plucked[$row[$key]] = $row[$column];
        }

        return $plucked;
    }

    public function exists(): bool
    {
        return $this->count() > 0;
    }

    public function doesntExist(): bool
    {
        return ! $this->exists();
    }

    // ==================== AGGREGATES ====================

    public function count(string $column = '*'): int
    {
        return (int) $this->aggregate('count', $column);
    }

    public function sum(string $column): float|int
    {
        return $this->aggregate('sum', $column) ?? 0;
    }

    public function avg(string $column): float|int|null
    {
        return $this->aggregate('avg', $column);
    }

    public function min(string $column): mixed
    {
        return $this->aggregate('min', $column);
    }

    public function max(string $column): mixed
    {
        return $this->aggregate('max', $column);
    }

    public function aggregate(string $function, string $column = '*'): mixed
    {
        $query = $this->cloneWithout(['columns', 'orders', 'limit', 'offset']);
        $query->columns = [new Raw("{$function}({$column}) as aggregate")];

        $result = $query->first();

        return $result['aggregate'] ?? null;
    }

    // ==================== CHUNKING ====================

    public function chunk(int $count, callable $callback): bool
    {
        $page = 1;
        $originalLimit = $this->limit;
        $originalOffset = $this->offset ?? 0;

        $processed = 0;

        while (true) {
            // Calculate chunk size
            $chunkSize = $count;
            if ($originalLimit !== null) {
                $remaining = $originalLimit - $processed;
                if ($remaining <= 0) {
                    break;
                }
                $chunkSize = min($count, $remaining);
            }

            $results = (clone $this)
                ->limit($chunkSize)
                ->offset($originalOffset + $processed)
                ->get();

            $resultsCount = count($results);

            if ($resultsCount === 0) {
                break;
            }

            if ($callback($results, $page) === false) {
                return false;
            }

            $processed += $resultsCount;
            $page++;

            // If we got fewer results than requested, we've reached the end
            if ($resultsCount < $chunkSize) {
                break;
            }

            // If we've processed all rows up to the original limit, stop
            if ($originalLimit !== null && $processed >= $originalLimit) {
                break;
            }
        }

        return true;
    }

    public function each(callable $callback, int $count = 1000): bool
    {
        return $this->chunk($count, function ($results) use ($callback) {
            foreach ($results as $key => $value) {
                if ($callback($value, $key) === false) {
                    return false;
                }
            }

            return true;
        });
    }

    public function lazy(int $chunkSize = 1000): Generator
    {
        $page = 0;
        $originalLimit = $this->limit;
        $totalYielded = 0;

        while (true) {
            $clone = clone $this;

            // Calculate the effective limit for this chunk
            $effectiveChunkSize = $chunkSize;
            if ($originalLimit !== null) {
                $remaining = $originalLimit - $totalYielded;
                if ($remaining <= 0) {
                    break;
                }
                $effectiveChunkSize = min($chunkSize, $remaining);
            }

            $clone->limit = $effectiveChunkSize;
            $clone->offset = ($page * $chunkSize) + ($this->offset ?? 0);

            $results = $clone->get();

            foreach ($results as $result) {
                yield $result;
                $totalYielded++;

                // Stop if we've yielded all rows up to the original limit
                if ($originalLimit !== null && $totalYielded >= $originalLimit) {
                    return;
                }
            }

            if (count($results) < $effectiveChunkSize) {
                break;
            }

            $page++;
        }
    }

    // ==================== UTILITY ====================

    public function clone(): static
    {
        return clone $this;
    }

    public function cloneWithout(array $properties): static
    {
        $clone = clone $this;

        foreach ($properties as $property) {
            if (property_exists($clone, $property)) {
                $clone->{$property} = match (gettype($clone->{$property})) {
                    'array' => [],
                    'boolean' => false,
                    default => null,
                };
            }
        }

        return $clone;
    }

    public function tap(callable $callback): static
    {
        $callback($this);

        return $this;
    }

    public function dump(): static
    {
        var_dump([
            'sql' => $this->toSql(),
            'bindings' => $this->getBindings(),
        ]);

        return $this;
    }

    public function dd(): never
    {
        $this->dump();
        exit(1);
    }

    public function getBindings(): array
    {
        return $this->bindings;
    }

    public function addBinding(mixed $value, string $type = 'where'): static
    {
        if (is_array($value)) {
            $this->bindings = array_merge($this->bindings, array_values($value));
        } elseif ($value !== null) {
            $this->bindings[] = $value;
        }

        return $this;
    }

    public function getClient(): ?ClientInterface
    {
        return $this->client;
    }

    public function setClient(?ClientInterface $client): static
    {
        $this->client = $client;

        return $this;
    }

    public function getGrammar(): Grammar
    {
        return $this->grammar;
    }

    protected function newJoinClause(string $type, string|Raw $table): JoinClause
    {
        return new JoinClause($this, $type, $table);
    }

    protected function isOperator(mixed $value): bool
    {
        if ( ! is_string($value)) {
            return false;
        }

        return in_array(mb_strtoupper($value), self::OPERATORS, true);
    }

    protected function prepareValueAndOperator(mixed $value, mixed $operator, bool $useDefault = false): array
    {
        if ($useDefault) {
            return [$operator, '='];
        }

        return [$value, $operator];
    }

    protected function preWhereNested(Closure $callback, string $boolean = 'AND'): static
    {
        $query = $this->forNestedWhere();
        $callback($query);

        if (count($query->preWheres) > 0) {
            $this->preWheres[] = [
                'type' => 'nested',
                'query' => $query,
                'boolean' => $boolean,
            ];

            $this->addBinding($query->bindings, 'prewhere');
        }

        return $this;
    }

    protected function havingNested(Closure $callback, string $boolean = 'AND'): static
    {
        $query = $this->forNestedWhere();
        $callback($query);

        if (count($query->havings) > 0) {
            $this->havings[] = [
                'type' => 'nested',
                'query' => $query,
                'boolean' => $boolean,
            ];

            $this->addBinding($query->bindings, 'having');
        }

        return $this;
    }

    protected function createSub(Closure|self $query): string
    {
        if ($query instanceof Closure) {
            $callback = $query;
            $query = $this->forSubQuery();
            $callback($query);
        }

        // Merge bindings from the subquery into the parent query
        $this->addBinding($query->getBindings(), 'where');

        return $query->toSql();
    }
}
