<?php

declare(strict_types=1);

namespace Flarme\PhpClickhouse\Tests\Benchmark;

use Flarme\PhpClickhouse\Database\Query\Builder;
use PhpBench\Attributes\Iterations;
use PhpBench\Attributes\Revs;

#[Revs(1000), Iterations(10)]
class QueryBuilderBench
{
    public function benchQueryBuilder(): void
    {
        (new Builder())
            ->select([
                'users.id',
                'users.name',
                'profiles.age',
                'COUNT(orders.id) as order_count',
                'SUM(orders.amount) as total_amount',
            ])
            ->from('users')
            ->join('profiles', 'users.id', '=', 'profiles.user_id')
            ->leftJoin('orders', 'users.id', '=', 'orders.user_id')
            ->where('users.created_at', '>=', '2023-01-01')
            ->whereIn('users.status', ['active', 'pending', 'verified'])
            ->whereNotNull('users.email')
            ->groupBy('users.id', 'users.name', 'profiles.age')
            ->having('order_count', '>', 5)
            ->orderBy('total_amount', 'DESC')
            ->orderBy('users.name', 'ASC')
            ->limit(100)
            ->offset(50)
            ->toSql();
    }

    public function benchQueryBuilderWithSubqueries(): void
    {
        (new Builder())
            ->with('active_users', function (Builder $query): void {
                $query->select(['id', 'name', 'email'])
                    ->from('users')
                    ->where('status', '=', 'active')
                    ->where('created_at', '>', '2024-01-01');
            })
            ->select(['au.id', 'au.name', 'order_stats.total'])
            ->from('active_users', 'au')
            ->joinSub(
                function (Builder $query): void {
                    $query->selectRaw('user_id, SUM(amount) as total, COUNT(*) as cnt')
                        ->from('orders')
                        ->where('status', '=', 'completed')
                        ->groupBy('user_id');
                },
                'order_stats',
                'au.id',
                '=',
                'order_stats.user_id'
            )
            ->where('order_stats.total', '>', 1000)
            ->orderByDesc('order_stats.total')
            ->limit(50)
            ->toSql();
    }
}
