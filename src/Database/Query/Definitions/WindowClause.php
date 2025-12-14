<?php

declare(strict_types=1);

namespace Flarme\PhpClickhouse\Database\Query\Definitions;

use Flarme\PhpClickhouse\Expressions\Raw;

class WindowClause
{
    /**
     * Frame bound: unbounded preceding.
     */
    public const UNBOUNDED_PRECEDING = 'UNBOUNDED PRECEDING';

    /**
     * Frame bound: current row.
     */
    public const CURRENT_ROW = 'CURRENT ROW';

    /**
     * Frame bound: unbounded following.
     */
    public const UNBOUNDED_FOLLOWING = 'UNBOUNDED FOLLOWING';

    public array $partitions = [];

    public array $orders = [];

    public ?array $frame = null;

    public function partitionBy(string|Raw ...$columns): static
    {
        foreach ($columns as $column) {
            $this->partitions[] = $column;
        }

        return $this;
    }

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

    public function rows(string $start, string $end): static
    {
        $this->frame = [
            'type' => 'ROWS',
            'start' => $start,
            'end' => $end,
        ];

        return $this;
    }

    public function range(string $start, string $end): static
    {
        $this->frame = [
            'type' => 'RANGE',
            'start' => $start,
            'end' => $end,
        ];

        return $this;
    }

    public function rowsBetween(string $start, string $end): static
    {
        return $this->rows($start, $end);
    }

    public function rangeBetween(string $start, string $end): static
    {
        return $this->range($start, $end);
    }
}
