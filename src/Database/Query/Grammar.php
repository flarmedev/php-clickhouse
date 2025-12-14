<?php

declare(strict_types=1);

namespace Flarme\PhpClickhouse\Database\Query;

use Flarme\PhpClickhouse\Concerns\EncodesSql;
use Flarme\PhpClickhouse\Database\Query\Definitions\JoinClause;
use Flarme\PhpClickhouse\Database\Query\Definitions\WindowClause;

class Grammar
{
    use EncodesSql;

    protected array $selectComponents = [
        'ctes',
        'columns',
        'from',
        'sample',
        'arrayJoins',
        'joins',
        'preWheres',
        'wheres',
        'groups',
        'havings',
        'windows',
        'qualifies',
        'orders',
        'limit',
        'offset',
        'limitBy',
        'unions',
        'intersects',
    ];

    public function compileSelect(Builder $query): string
    {
        if (empty($query->columns)) {
            $query->columns = ['*'];
        }

        $sql = mb_trim($this->concatenate(
            $this->compileComponents($query)
        ));

        return $sql;
    }

    protected function compileComponents(Builder $query): array
    {
        $sql = [];

        foreach ($this->selectComponents as $component) {
            $method = 'compile' . ucfirst($component);

            if (method_exists($this, $method)) {
                $result = $this->{$method}($query);
                if ($result !== null && $result !== '') {
                    $sql[$component] = $result;
                }
            }
        }

        return $sql;
    }

    protected function compileCtes(Builder $query): ?string
    {
        if (empty($query->ctes)) {
            return null;
        }

        $ctes = [];

        foreach ($query->ctes as $cte) {
            $subQuery = $cte['query'];
            $ctes[] = $this->wrap($cte['name']) . ' AS (' . $subQuery . ')';
        }

        return 'WITH ' . implode(', ', $ctes);
    }

    protected function compileColumns(Builder $query): string
    {
        $select = $query->distinct ? 'SELECT DISTINCT ' : 'SELECT ';

        return $select . $this->columnize($query->columns);
    }

    protected function compileFrom(Builder $query): ?string
    {
        if ($query->from === null) {
            return null;
        }

        $from = $this->wrap($query->from);

        if ($query->fromAlias) {
            $from .= ' AS ' . $this->wrap($query->fromAlias);
        }

        if ($query->final) {
            $from .= ' FINAL';
        }

        return 'FROM ' . $from;
    }

    protected function compileSample(Builder $query): ?string
    {
        if ($query->sample === null) {
            return null;
        }

        $sql = 'SAMPLE ' . $query->sample['sample'];

        if ($query->sample['offset'] !== null) {
            $sql .= ' OFFSET ' . $query->sample['offset'];
        }

        return $sql;
    }

    protected function compileArrayJoins(Builder $query): ?string
    {
        if (empty($query->arrayJoins)) {
            return null;
        }

        $joins = [];

        foreach ($query->arrayJoins as $join) {
            $sql = $join['type'] . ' ' . $this->wrap($join['column']);

            if ($join['alias']) {
                $sql .= ' AS ' . $this->wrap($join['alias']);
            }

            $joins[] = $sql;
        }

        return implode(' ', $joins);
    }

    protected function compileJoins(Builder $query): ?string
    {
        if (empty($query->joins)) {
            return null;
        }

        $joins = [];

        foreach ($query->joins as $join) {
            $table = $this->wrap($join->table);

            if ($join->alias) {
                $table .= ' AS ' . $this->wrap($join->alias);
            }

            $sql = mb_trim($join->type) . ' JOIN ' . $table;

            if ( ! empty($join->clauses)) {
                $sql .= ' ON ' . $this->compileJoinClauses($join);
            }

            if ( ! empty($join->using)) {
                $sql .= ' USING (' . $this->columnize($join->using) . ')';
            }

            $joins[] = $sql;
        }

        return implode(' ', $joins);
    }

    protected function compileJoinClauses(JoinClause $join): string
    {
        $clauses = [];

        foreach ($join->clauses as $index => $clause) {
            $boolean = $index === 0 ? '' : $clause['boolean'] . ' ';

            if (($clause['type'] ?? null) === 'nested') {
                $clauses[] = $boolean . '(' . $this->compileJoinClauses($clause['join']) . ')';
            } else {
                $first = $this->wrap($clause['first']);
                $second = $this->wrap($clause['second']);
                $clauses[] = $boolean . "{$first} {$clause['operator']} {$second}";
            }
        }

        return implode(' ', $clauses);
    }

    protected function compilePreWheres(Builder $query): ?string
    {
        if (empty($query->preWheres)) {
            return null;
        }

        return 'PREWHERE ' . $this->compileConditions($query->preWheres, $query);
    }

    protected function compileWheres(Builder $query): ?string
    {
        if (empty($query->wheres)) {
            return null;
        }

        return 'WHERE ' . $this->compileConditions($query->wheres, $query);
    }

    protected function compileConditions(array $conditions, Builder $query): string
    {
        $sql = [];

        foreach ($conditions as $index => $where) {
            $boolean = $index === 0 ? '' : $where['boolean'] . ' ';
            $sql[] = $boolean . $this->compileCondition($where, $query);
        }

        return implode(' ', $sql);
    }

    protected function compileCondition(array $where, Builder $query): string
    {
        $type = $where['type'] ?? 'basic';

        return match ($type) {
            'raw' => $where['sql'],
            'nested' => '(' . $this->compileConditions(
                $where['query']->wheres ?: $where['query']->preWheres ?: $where['query']->havings,
                $query
            ) . ')',
            'in' => $this->compileWhereIn($where),
            'inSub' => $this->compileWhereInSub($where),
            'globalIn' => $this->compileWhereGlobalIn($where),
            'globalInSub' => $this->compileWhereGlobalInSub($where),
            'between' => $this->compileWhereBetween($where),
            'null' => $this->compileWhereNull($where),
            'column' => $this->compileWhereColumn($where),
            'exists' => $this->compileWhereExists($where),
            default => $this->compileWhereBasic($where),
        };
    }

    protected function compileWhereBasic(array $where): string
    {
        $column = $this->wrap($where['column']);

        return "{$column} {$where['operator']} ?";
    }

    protected function compileWhereIn(array $where): string
    {
        $column = $this->wrap($where['column']);
        $not = $where['not'] ? 'NOT ' : '';
        $placeholders = implode(', ', array_fill(0, count($where['values']), '?'));

        return "{$column} {$not}IN ({$placeholders})";
    }

    protected function compileWhereInSub(array $where): string
    {
        $column = $this->wrap($where['column']);
        $not = $where['not'] ? 'NOT ' : '';

        return "{$column} {$not}IN ({$where['query']})";
    }

    protected function compileWhereGlobalIn(array $where): string
    {
        $column = $this->wrap($where['column']);
        $not = $where['not'] ? 'NOT ' : '';
        $placeholders = implode(', ', array_fill(0, count($where['values']), '?'));

        return "{$column} GLOBAL {$not}IN ({$placeholders})";
    }

    protected function compileWhereGlobalInSub(array $where): string
    {
        $column = $this->wrap($where['column']);
        $not = $where['not'] ? 'NOT ' : '';

        return "{$column} GLOBAL {$not}IN ({$where['query']})";
    }

    protected function compileWhereBetween(array $where): string
    {
        $column = $this->wrap($where['column']);
        $not = $where['not'] ? 'NOT ' : '';

        return "{$column} {$not}BETWEEN ? AND ?";
    }

    protected function compileWhereNull(array $where): string
    {
        $column = $this->wrap($where['column']);

        return $where['not']
            ? "{$column} IS NOT NULL"
            : "{$column} IS NULL";
    }

    protected function compileWhereColumn(array $where): string
    {
        $first = $this->wrap($where['first']);
        $second = $this->wrap($where['second']);

        return "{$first} {$where['operator']} {$second}";
    }

    protected function compileWhereExists(array $where): string
    {
        $not = $where['not'] ? 'NOT ' : '';

        return "{$not}EXISTS ({$where['query']})";
    }

    protected function compileGroups(Builder $query): ?string
    {
        if (empty($query->groups)) {
            return null;
        }

        $sql = 'GROUP BY ' . $this->columnize($query->groups);

        if ( ! empty($query->groupModifiers)) {
            if ($query->groupModifiers['rollup'] ?? false) {
                $sql .= ' WITH ROLLUP';
            } elseif ($query->groupModifiers['cube'] ?? false) {
                $sql .= ' WITH CUBE';
            }

            if ($query->groupModifiers['totals'] ?? false) {
                $sql .= ' WITH TOTALS';
            }
        }

        return $sql;
    }

    protected function compileHavings(Builder $query): ?string
    {
        if (empty($query->havings)) {
            return null;
        }

        return 'HAVING ' . $this->compileConditions($query->havings, $query);
    }

    protected function compileWindows(Builder $query): ?string
    {
        if (empty($query->windows)) {
            return null;
        }

        $windows = [];

        foreach ($query->windows as $window) {
            $windows[] = $this->wrap($window['name']) . ' AS (' . $this->compileWindowDefinition($window['definition']) . ')';
        }

        return 'WINDOW ' . implode(', ', $windows);
    }

    protected function compileWindowDefinition(WindowClause $window): string
    {
        $parts = [];

        if ( ! empty($window->partitions)) {
            $parts[] = 'PARTITION BY ' . $this->columnize($window->partitions);
        }

        if ( ! empty($window->orders)) {
            $orders = [];
            foreach ($window->orders as $order) {
                $orders[] = $this->wrap($order['column']) . ' ' . $order['direction'];
            }
            $parts[] = 'ORDER BY ' . implode(', ', $orders);
        }

        if ($window->frame !== null) {
            $parts[] = "{$window->frame['type']} BETWEEN {$window->frame['start']} AND {$window->frame['end']}";
        }

        return implode(' ', $parts);
    }

    protected function compileQualifies(Builder $query): ?string
    {
        if (empty($query->qualifies)) {
            return null;
        }

        return 'QUALIFY ' . $this->compileConditions($query->qualifies, $query);
    }

    protected function compileOrders(Builder $query): ?string
    {
        if (empty($query->orders)) {
            return null;
        }

        $orders = [];

        foreach ($query->orders as $order) {
            if (($order['type'] ?? null) === 'raw') {
                $orders[] = $order['sql'];
            } else {
                $orders[] = $this->wrap($order['column']) . ' ' . $order['direction'];
            }
        }

        return 'ORDER BY ' . implode(', ', $orders);
    }

    protected function compileLimit(Builder $query): ?string
    {
        if ($query->limit === null) {
            return null;
        }

        return 'LIMIT ' . $query->limit;
    }

    protected function compileOffset(Builder $query): ?string
    {
        if ($query->offset === null) {
            return null;
        }

        return 'OFFSET ' . $query->offset;
    }

    protected function compileLimitBy(Builder $query): ?string
    {
        if ($query->limitBy === null) {
            return null;
        }

        $sql = 'LIMIT ' . $query->limitBy['limit'];

        if ($query->limitBy['offset'] !== null) {
            $sql .= ' OFFSET ' . $query->limitBy['offset'];
        }

        $sql .= ' BY ' . $this->columnize($query->limitBy['columns']);

        return $sql;
    }

    protected function compileUnions(Builder $query): ?string
    {
        if (empty($query->unions)) {
            return null;
        }

        $unions = [];

        foreach ($query->unions as $union) {
            $type = $union['all'] ? 'UNION ALL' : 'UNION DISTINCT';
            $unions[] = "{$type} ({$union['query']})";
        }

        return implode(' ', $unions);
    }

    protected function compileIntersects(Builder $query): ?string
    {
        if (empty($query->intersects)) {
            return null;
        }

        $intersects = [];

        foreach ($query->intersects as $intersect) {
            $type = $intersect['distinct'] ? 'INTERSECT DISTINCT' : 'INTERSECT';
            $intersects[] = "{$type} ({$intersect['query']})";
        }

        return implode(' ', $intersects);
    }

    protected function concatenate(array $segments): string
    {
        return implode(' ', array_filter($segments, fn($value) => $value !== '' && $value !== null));
    }

    protected function columnize(array $columns): string
    {
        return implode(', ', array_map([$this, 'wrap'], $columns));
    }
}
