<?php

declare(strict_types=1);

use Flarme\PhpClickhouse\Database\Exceptions\MissingClientException;
use Flarme\PhpClickhouse\Database\Query\Builder;
use Flarme\PhpClickhouse\Database\Query\Grammar;
use Flarme\PhpClickhouse\Expressions\Raw;
use Flarme\PhpClickhouse\Query;

describe('Builder', function (): void {
    describe('constants', function (): void {
        it('defines OPERATORS constant', function (): void {
            expect(Builder::OPERATORS)->toBe([
                '=', '!=', '<>', '<', '>', '<=', '>=',
                'LIKE', 'NOT LIKE', 'ILIKE', 'NOT ILIKE',
                'IN', 'NOT IN', 'BETWEEN', 'NOT BETWEEN',
            ]);
        });

        it('defines COLUMN_OPERATORS constant', function (): void {
            expect(Builder::COLUMN_OPERATORS)->toBe(['=', '!=', '<>', '<', '>', '<=', '>=']);
        });
    });

    describe('construction', function (): void {
        it('can be instantiated without client', function (): void {
            $builder = new Builder();
        })->throwsNoExceptions();

        it('initializes grammar', function (): void {
            $builder = new Builder();

            expect($builder->grammar)->toBeInstanceOf(Grammar::class);
        });

        it('creates new builder instance statically', function (): void {
            $builder = Builder::query();

            expect($builder)->toBeInstanceOf(Builder::class);
        });
    });

    describe('newQuery', function (): void {
        it('creates new builder with same client', function (): void {
            $builder = new Builder();
            $newBuilder = $builder->newQuery();

            expect($newBuilder)->toBeInstanceOf(Builder::class)
                ->and($newBuilder->client)->toBe($builder->client)
                ->and($newBuilder)->not->toBe($builder);
        });
    });

    describe('select', function (): void {
        it('sets columns from array', function (): void {
            $builder = Builder::query()->select(['id', 'name']);

            expect($builder->columns)->toBe(['id', 'name']);
        });

        it('sets columns from variadic arguments', function (): void {
            $builder = Builder::query()->select('id', 'name', 'email');

            expect($builder->columns)->toBe(['id', 'name', 'email']);
        });

        it('resets columns on each call', function (): void {
            $builder = Builder::query()->select('id')->select('name');

            expect($builder->columns)->toBe(['name']);
        });

        it('returns self for fluent interface', function (): void {
            $builder = Builder::query();
            $result = $builder->select('id');

            expect($result)->toBe($builder);
        });
    });

    describe('addSelect', function (): void {
        it('adds columns to existing selection', function (): void {
            $builder = Builder::query()->select('id')->addSelect('name');

            expect($builder->columns)->toBe(['id', 'name']);
        });

        it('adds multiple columns', function (): void {
            $builder = Builder::query()->select('id')->addSelect('name', 'email');

            expect($builder->columns)->toBe(['id', 'name', 'email']);
        });

        it('adds columns from array', function (): void {
            $builder = Builder::query()->select('id')->addSelect(['name', 'email']);

            expect($builder->columns)->toBe(['id', 'name', 'email']);
        });
    });

    describe('selectRaw', function (): void {
        it('adds raw expression to columns', function (): void {
            $builder = Builder::query()->selectRaw('COUNT(*) as total');

            expect($builder->columns[0])->toBeInstanceOf(Raw::class)
                ->and((string) $builder->columns[0])->toBe('COUNT(*) as total');
        });

        it('adds bindings', function (): void {
            $builder = Builder::query()->selectRaw('COUNT(*) as total WHERE id > ?', [10]);

            expect($builder->bindings)->toBe([10]);
        });
    });

    describe('distinct', function (): void {
        it('sets distinct flag', function (): void {
            $builder = Builder::query()->distinct();

            expect($builder->distinct)->toBeTrue();
        });
    });

    describe('from', function (): void {
        it('sets table name', function (): void {
            $builder = Builder::query()->from('users');

            expect($builder->from)->toBe('users');
        });

        it('sets table with alias', function (): void {
            $builder = Builder::query()->from('users', 'u');

            expect($builder->from)->toBe('users')
                ->and($builder->fromAlias)->toBe('u');
        });

        it('accepts Raw expression', function (): void {
            $raw = new Raw('system.tables');
            $builder = Builder::query()->from($raw);

            expect($builder->from)->toBe($raw);
        });

        it('accepts closure for subquery', function (): void {
            $builder = Builder::query()->from(function ($query): void {
                $query->from('users')->where('active', '=', true);
            });

            expect($builder->from)->toBeInstanceOf(Raw::class);
        });
    });

    describe('fromSub', function (): void {
        it('sets subquery as from', function (): void {
            $builder = Builder::query()->fromSub(function ($query): void {
                $query->from('users');
            }, 'u');

            expect($builder->from)->toBeInstanceOf(Raw::class)
                ->and($builder->fromAlias)->toBe('u');
        });
    });

    describe('as', function (): void {
        it('sets table alias', function (): void {
            $builder = Builder::query()->from('users')->as('u');

            expect($builder->fromAlias)->toBe('u');
        });
    });

    describe('final', function (): void {
        it('sets final flag', function (): void {
            $builder = Builder::query()->final();

            expect($builder->final)->toBeTrue();
        });

        it('can disable final', function (): void {
            $builder = Builder::query()->final()->final(false);

            expect($builder->final)->toBeFalse();
        });
    });

    describe('sample', function (): void {
        it('sets sample value', function (): void {
            $builder = Builder::query()->sample(0.1);

            expect($builder->sample)->toBe(['sample' => 0.1, 'offset' => null]);
        });

        it('sets sample with offset', function (): void {
            $builder = Builder::query()->sample(0.1, 0.5);

            expect($builder->sample)->toBe(['sample' => 0.1, 'offset' => 0.5]);
        });
    });

    describe('where', function (): void {
        it('adds basic where clause', function (): void {
            $builder = Builder::query()->where('id', '=', 1);

            expect($builder->wheres)->toHaveCount(1)
                ->and($builder->wheres[0]['column'])->toBe('id')
                ->and($builder->wheres[0]['operator'])->toBe('=')
                ->and($builder->wheres[0]['value'])->toBe(1);
        });

        it('uses equals operator by default', function (): void {
            $builder = Builder::query()->where('id', 1);

            expect($builder->wheres[0]['operator'])->toBe('=')
                ->and($builder->wheres[0]['value'])->toBe(1);
        });

        it('adds binding', function (): void {
            $builder = Builder::query()->where('id', '=', 1);

            expect($builder->bindings)->toBe([1]);
        });

        it('handles null value with equals as IS NULL', function (): void {
            $builder = Builder::query()->where('deleted_at', '=', null);

            expect($builder->wheres[0]['type'])->toBe('null')
                ->and($builder->wheres[0]['not'])->toBeFalse();
        });

        it('handles null value with not equals as IS NOT NULL', function (): void {
            $builder = Builder::query()->where('deleted_at', '!=', null);

            expect($builder->wheres[0]['type'])->toBe('null')
                ->and($builder->wheres[0]['not'])->toBeTrue();
        });

        it('handles closure for nested where', function (): void {
            $builder = Builder::query()->where(function ($query): void {
                $query->where('a', '=', 1)->where('b', '=', 2);
            });

            expect($builder->wheres[0]['type'])->toBe('nested');
        });
    });

    describe('orWhere', function (): void {
        it('adds where with OR boolean', function (): void {
            $builder = Builder::query()
                ->where('a', '=', 1)
                ->orWhere('b', '=', 2);

            expect($builder->wheres[1]['boolean'])->toBe('OR');
        });
    });

    describe('whereRaw', function (): void {
        it('adds raw where clause', function (): void {
            $builder = Builder::query()->whereRaw('id > 10');

            expect($builder->wheres[0]['type'])->toBe('raw')
                ->and($builder->wheres[0]['sql'])->toBe('id > 10');
        });

        it('adds bindings', function (): void {
            $builder = Builder::query()->whereRaw('id > ?', [10]);

            expect($builder->bindings)->toBe([10]);
        });
    });

    describe('whereIn', function (): void {
        it('adds whereIn clause', function (): void {
            $builder = Builder::query()->whereIn('id', [1, 2, 3]);

            expect($builder->wheres[0]['type'])->toBe('in')
                ->and($builder->wheres[0]['values'])->toBe([1, 2, 3]);
        });

        it('adds bindings for values', function (): void {
            $builder = Builder::query()->whereIn('id', [1, 2, 3]);

            expect($builder->bindings)->toBe([1, 2, 3]);
        });

        it('handles subquery', function (): void {
            $builder = Builder::query()->whereIn('id', function ($query): void {
                $query->select('user_id')->from('orders');
            });

            expect($builder->wheres[0]['type'])->toBe('inSub');
        });
    });

    describe('whereNotIn', function (): void {
        it('adds whereNotIn clause', function (): void {
            $builder = Builder::query()->whereNotIn('id', [1, 2]);

            expect($builder->wheres[0]['not'])->toBeTrue();
        });
    });

    describe('whereGlobalIn', function (): void {
        it('adds whereGlobalIn clause', function (): void {
            $builder = Builder::query()->whereGlobalIn('id', [1, 2]);

            expect($builder->wheres[0]['type'])->toBe('globalIn');
        });

        it('handles subquery', function (): void {
            $builder = Builder::query()->whereGlobalIn('id', function ($query): void {
                $query->select('id')->from('remote_users');
            });

            expect($builder->wheres[0]['type'])->toBe('globalInSub');
        });
    });

    describe('whereBetween', function (): void {
        it('adds whereBetween clause', function (): void {
            $builder = Builder::query()->whereBetween('age', [18, 65]);

            expect($builder->wheres[0]['type'])->toBe('between')
                ->and($builder->wheres[0]['values'])->toBe([18, 65]);
        });

        it('adds only first two values as bindings', function (): void {
            $builder = Builder::query()->whereBetween('age', [18, 65, 100]);

            expect($builder->bindings)->toBe([18, 65]);
        });
    });

    describe('whereNull', function (): void {
        it('adds whereNull clause', function (): void {
            $builder = Builder::query()->whereNull('deleted_at');

            expect($builder->wheres[0]['type'])->toBe('null')
                ->and($builder->wheres[0]['not'])->toBeFalse();
        });
    });

    describe('whereNotNull', function (): void {
        it('adds whereNotNull clause', function (): void {
            $builder = Builder::query()->whereNotNull('email');

            expect($builder->wheres[0]['type'])->toBe('null')
                ->and($builder->wheres[0]['not'])->toBeTrue();
        });
    });

    describe('whereColumn', function (): void {
        it('adds whereColumn clause', function (): void {
            $builder = Builder::query()->whereColumn('created_at', '=', 'updated_at');

            expect($builder->wheres[0]['type'])->toBe('column')
                ->and($builder->wheres[0]['first'])->toBe('created_at')
                ->and($builder->wheres[0]['second'])->toBe('updated_at');
        });

        it('uses equals operator by default', function (): void {
            $builder = Builder::query()->whereColumn('a', 'b');

            expect($builder->wheres[0]['operator'])->toBe('=');
        });
    });

    describe('whereExists', function (): void {
        it('adds whereExists clause', function (): void {
            $builder = Builder::query()->whereExists(function ($query): void {
                $query->from('orders');
            });

            expect($builder->wheres[0]['type'])->toBe('exists')
                ->and($builder->wheres[0]['not'])->toBeFalse();
        });
    });

    describe('whereNotExists', function (): void {
        it('adds whereNotExists clause', function (): void {
            $builder = Builder::query()->whereNotExists(function ($query): void {
                $query->from('orders');
            });

            expect($builder->wheres[0]['not'])->toBeTrue();
        });
    });

    describe('whereLike', function (): void {
        it('adds whereLike clause', function (): void {
            $builder = Builder::query()->whereLike('name', '%John%');

            expect($builder->wheres[0]['operator'])->toBe('LIKE')
                ->and($builder->wheres[0]['value'])->toBe('%John%');
        });
    });

    describe('whereNotLike', function (): void {
        it('adds whereNotLike clause', function (): void {
            $builder = Builder::query()->whereNotLike('name', '%test%');

            expect($builder->wheres[0]['operator'])->toBe('NOT LIKE');
        });
    });

    describe('preWhere', function (): void {
        it('adds preWhere clause', function (): void {
            $builder = Builder::query()->preWhere('date', '=', '2024-01-01');

            expect($builder->preWheres)->toHaveCount(1)
                ->and($builder->preWheres[0]['column'])->toBe('date');
        });

        it('handles closure for nested preWhere', function (): void {
            $builder = Builder::query()->preWhere(function ($query): void {
                $query->preWhere('a', '=', 1);
            });

            expect($builder->preWheres[0]['type'])->toBe('nested');
        });
    });

    describe('join', function (): void {
        it('adds join clause', function (): void {
            $builder = Builder::query()->join('orders', 'users.id', '=', 'orders.user_id');

            expect($builder->joins)->toHaveCount(1)
                ->and($builder->joins[0]->type)->toBe('INNER');
        });

        it('handles closure for complex join', function (): void {
            $builder = Builder::query()->join('orders', function ($join): void {
                $join->on('users.id', '=', 'orders.user_id')
                    ->on('users.status', '=', 'orders.status');
            });

            expect($builder->joins[0]->clauses)->toHaveCount(2);
        });
    });

    describe('leftJoin', function (): void {
        it('adds left join', function (): void {
            $builder = Builder::query()->leftJoin('orders', 'users.id', '=', 'orders.user_id');

            expect($builder->joins[0]->type)->toBe('LEFT');
        });
    });

    describe('rightJoin', function (): void {
        it('adds right join', function (): void {
            $builder = Builder::query()->rightJoin('orders', 'users.id', '=', 'orders.user_id');

            expect($builder->joins[0]->type)->toBe('RIGHT');
        });
    });

    describe('crossJoin', function (): void {
        it('adds cross join', function (): void {
            $builder = Builder::query()->crossJoin('categories');

            expect($builder->joins[0]->type)->toBe('CROSS');
        });
    });

    describe('asofJoin', function (): void {
        it('adds ASOF join', function (): void {
            $builder = Builder::query()->asofJoin('prices', 'products.id', '=', 'prices.product_id');

            expect($builder->joins[0]->type)->toBe('ASOF');
        });
    });

    describe('globalJoin', function (): void {
        it('adds GLOBAL join', function (): void {
            $builder = Builder::query()->globalJoin('remote_users', 'users.id', '=', 'remote_users.id');

            expect($builder->joins[0]->type)->toBe('GLOBAL INNER');
        });
    });

    describe('arrayJoin', function (): void {
        it('adds ARRAY JOIN', function (): void {
            $builder = Builder::query()->arrayJoin('tags');

            expect($builder->arrayJoins)->toHaveCount(1)
                ->and($builder->arrayJoins[0]['type'])->toBe('ARRAY JOIN');
        });

        it('adds ARRAY JOIN with alias', function (): void {
            $builder = Builder::query()->arrayJoin('tags', 't');

            expect($builder->arrayJoins[0]['alias'])->toBe('t');
        });
    });

    describe('leftArrayJoin', function (): void {
        it('adds LEFT ARRAY JOIN', function (): void {
            $builder = Builder::query()->leftArrayJoin('tags');

            expect($builder->arrayJoins[0]['type'])->toBe('LEFT ARRAY JOIN');
        });
    });

    describe('groupBy', function (): void {
        it('adds group by columns', function (): void {
            $builder = Builder::query()->groupBy('status');

            expect($builder->groups)->toBe(['status']);
        });

        it('adds multiple group by columns', function (): void {
            $builder = Builder::query()->groupBy('status', 'role');

            expect($builder->groups)->toBe(['status', 'role']);
        });
    });

    describe('groupByRaw', function (): void {
        it('adds raw group by', function (): void {
            $builder = Builder::query()->groupByRaw('YEAR(created_at)');

            expect($builder->groups[0])->toBeInstanceOf(Raw::class);
        });
    });

    describe('withRollup', function (): void {
        it('sets rollup modifier', function (): void {
            $builder = Builder::query()->groupBy('status')->withRollup();

            expect($builder->groupModifiers['rollup'])->toBeTrue();
        });
    });

    describe('withCube', function (): void {
        it('sets cube modifier', function (): void {
            $builder = Builder::query()->groupBy('status')->withCube();

            expect($builder->groupModifiers['cube'])->toBeTrue();
        });
    });

    describe('withTotals', function (): void {
        it('sets totals modifier', function (): void {
            $builder = Builder::query()->groupBy('status')->withTotals();

            expect($builder->groupModifiers['totals'])->toBeTrue();
        });
    });

    describe('having', function (): void {
        it('adds having clause', function (): void {
            $builder = Builder::query()->having('count', '>', 10);

            expect($builder->havings)->toHaveCount(1);
        });

        it('handles closure for nested having', function (): void {
            $builder = Builder::query()->having(function ($query): void {
                $query->having('a', '>', 1);
            });

            expect($builder->havings[0]['type'])->toBe('nested');
        });
    });

    describe('havingRaw', function (): void {
        it('adds raw having clause', function (): void {
            $builder = Builder::query()->havingRaw('COUNT(*) > 10');

            expect($builder->havings[0]['type'])->toBe('raw');
        });
    });

    describe('orderBy', function (): void {
        it('adds order by clause', function (): void {
            $builder = Builder::query()->orderBy('name');

            expect($builder->orders)->toHaveCount(1)
                ->and($builder->orders[0]['column'])->toBe('name')
                ->and($builder->orders[0]['direction'])->toBe('ASC');
        });

        it('normalizes direction to uppercase', function (): void {
            $builder = Builder::query()->orderBy('name', 'desc');

            expect($builder->orders[0]['direction'])->toBe('DESC');
        });
    });

    describe('orderByDesc', function (): void {
        it('adds descending order', function (): void {
            $builder = Builder::query()->orderByDesc('created_at');

            expect($builder->orders[0]['direction'])->toBe('DESC');
        });
    });

    describe('orderByRaw', function (): void {
        it('adds raw order by', function (): void {
            $builder = Builder::query()->orderByRaw('RAND()');

            expect($builder->orders[0]['type'])->toBe('raw');
        });
    });

    describe('latest', function (): void {
        it('orders by created_at DESC by default', function (): void {
            $builder = Builder::query()->latest();

            expect($builder->orders[0]['column'])->toBe('created_at')
                ->and($builder->orders[0]['direction'])->toBe('DESC');
        });

        it('accepts custom column', function (): void {
            $builder = Builder::query()->latest('updated_at');

            expect($builder->orders[0]['column'])->toBe('updated_at');
        });
    });

    describe('oldest', function (): void {
        it('orders by created_at ASC by default', function (): void {
            $builder = Builder::query()->oldest();

            expect($builder->orders[0]['column'])->toBe('created_at')
                ->and($builder->orders[0]['direction'])->toBe('ASC');
        });
    });

    describe('reorder', function (): void {
        it('clears existing orders', function (): void {
            $builder = Builder::query()->orderBy('name')->reorder();

            expect($builder->orders)->toBe([]);
        });

        it('can set new order', function (): void {
            $builder = Builder::query()->orderBy('name')->reorder('id');

            expect($builder->orders)->toHaveCount(1)
                ->and($builder->orders[0]['column'])->toBe('id');
        });
    });

    describe('limit', function (): void {
        it('sets limit', function (): void {
            $builder = Builder::query()->limit(10);

            expect($builder->limit)->toBe(10);
        });

        it('ensures non-negative limit', function (): void {
            $builder = Builder::query()->limit(-5);

            expect($builder->limit)->toBe(0);
        });
    });

    describe('take', function (): void {
        it('is alias for limit', function (): void {
            $builder = Builder::query()->take(10);

            expect($builder->limit)->toBe(10);
        });
    });

    describe('offset', function (): void {
        it('sets offset', function (): void {
            $builder = Builder::query()->offset(20);

            expect($builder->offset)->toBe(20);
        });

        it('ensures non-negative offset', function (): void {
            $builder = Builder::query()->offset(-5);

            expect($builder->offset)->toBe(0);
        });
    });

    describe('skip', function (): void {
        it('is alias for offset', function (): void {
            $builder = Builder::query()->skip(20);

            expect($builder->offset)->toBe(20);
        });
    });

    describe('forPage', function (): void {
        it('sets limit and offset for pagination', function (): void {
            $builder = Builder::query()->forPage(3, 15);

            expect($builder->limit)->toBe(15)
                ->and($builder->offset)->toBe(30);
        });
    });

    describe('limitBy', function (): void {
        it('sets limit by clause', function (): void {
            $builder = Builder::query()->limitBy(5, 'user_id');

            expect($builder->limitBy)->toBe([
                'limit' => 5,
                'offset' => null,
                'columns' => ['user_id'],
            ]);
        });

        it('accepts multiple columns', function (): void {
            $builder = Builder::query()->limitBy(5, 'user_id', 'category');

            expect($builder->limitBy['columns'])->toBe(['user_id', 'category']);
        });
    });

    describe('limitByOffset', function (): void {
        it('sets limit by with offset', function (): void {
            $builder = Builder::query()->limitByOffset(5, 10, 'user_id');

            expect($builder->limitBy)->toBe([
                'limit' => 5,
                'offset' => 10,
                'columns' => ['user_id'],
            ]);
        });
    });

    describe('union', function (): void {
        it('adds union clause', function (): void {
            $builder = Builder::query()->from('users')
                ->union(function ($query): void {
                    $query->from('admins');
                });

            expect($builder->unions)->toHaveCount(1)
                ->and($builder->unions[0]['all'])->toBeTrue();
        });

        it('can add union distinct', function (): void {
            $builder = Builder::query()->from('users')
                ->union(function ($query): void {
                    $query->from('admins');
                }, false);

            expect($builder->unions[0]['all'])->toBeFalse();
        });
    });

    describe('unionAll', function (): void {
        it('adds union all clause', function (): void {
            $builder = Builder::query()->from('users')
                ->unionAll(function ($query): void {
                    $query->from('admins');
                });

            expect($builder->unions[0]['all'])->toBeTrue();
        });
    });

    describe('intersect', function (): void {
        it('adds intersect clause', function (): void {
            $builder = Builder::query()->from('users')
                ->intersect(function ($query): void {
                    $query->from('active_users');
                });

            expect($builder->intersects)->toHaveCount(1);
        });
    });

    describe('with', function (): void {
        it('adds CTE', function (): void {
            $builder = Builder::query()
                ->with('active_users', function ($query): void {
                    $query->from('users')->where('active', '=', true);
                });

            expect($builder->ctes)->toHaveCount(1)
                ->and($builder->ctes[0]['name'])->toBe('active_users');
        });
    });

    describe('window', function (): void {
        it('adds window definition', function (): void {
            $builder = Builder::query()->window('w', function ($window): void {
                $window->partitionBy('user_id');
            });

            expect($builder->windows)->toHaveCount(1)
                ->and($builder->windows[0]['name'])->toBe('w');
        });
    });

    describe('qualify', function (): void {
        it('adds qualify clause', function (): void {
            $builder = Builder::query()->qualify('row_num', '=', 1);

            expect($builder->qualifies)->toHaveCount(1);
        });
    });

    describe('when', function (): void {
        it('executes callback when condition is true', function (): void {
            $builder = Builder::query()->when(true, function ($query): void {
                $query->where('active', '=', true);
            });

            expect($builder->wheres)->toHaveCount(1);
        });

        it('does not execute callback when condition is false', function (): void {
            $builder = Builder::query()->when(false, function ($query): void {
                $query->where('active', '=', true);
            });

            expect($builder->wheres)->toHaveCount(0);
        });

        it('executes default callback when condition is false', function (): void {
            $builder = Builder::query()->when(false, function ($query): void {
                $query->where('active', '=', true);
            }, function ($query): void {
                $query->where('inactive', '=', true);
            });

            expect($builder->wheres)->toHaveCount(1)
                ->and($builder->wheres[0]['column'])->toBe('inactive');
        });

        it('evaluates closure condition', function (): void {
            $builder = Builder::query()->when(fn() => true, function ($query): void {
                $query->where('active', '=', true);
            });

            expect($builder->wheres)->toHaveCount(1);
        });
    });

    describe('unless', function (): void {
        it('executes callback when condition is false', function (): void {
            $builder = Builder::query()->unless(false, function ($query): void {
                $query->where('active', '=', true);
            });

            expect($builder->wheres)->toHaveCount(1);
        });

        it('does not execute callback when condition is true', function (): void {
            $builder = Builder::query()->unless(true, function ($query): void {
                $query->where('active', '=', true);
            });

            expect($builder->wheres)->toHaveCount(0);
        });
    });

    describe('toSql', function (): void {
        it('returns compiled SQL', function (): void {
            $builder = Builder::query()->select('id', 'name')->from('users');

            expect($builder->toSql())->toBe('SELECT `id`, `name` FROM `users`');
        });
    });

    describe('toQuery', function (): void {
        it('returns Query instance', function (): void {
            $builder = Builder::query()->from('users')->where('id', '=', 1);

            $query = $builder->toQuery();

            expect($query)->toBeInstanceOf(Query::class)
                ->and($query->sql)->toBe('SELECT * FROM `users` WHERE `id` = ?')
                ->and($query->bindings)->toBe([1]);
        });
    });

    describe('toRawSql', function (): void {
        it('returns SQL with substituted bindings', function (): void {
            $builder = Builder::query()->from('users')->where('id', '=', 1);

            expect($builder->toRawSql())->toBe('SELECT * FROM `users` WHERE `id` = 1');
        });
    });

    describe('execute', function (): void {
        it('throws MissingClientException when no client', function (): void {
            $builder = Builder::query()->from('users');

            expect(fn() => $builder->execute())->toThrow(MissingClientException::class);
        });
    });

    describe('getBindings', function (): void {
        it('returns all bindings', function (): void {
            $builder = Builder::query()
                ->where('a', '=', 1)
                ->where('b', '=', 2);

            expect($builder->getBindings())->toBe([1, 2]);
        });
    });

    describe('addBinding', function (): void {
        it('adds single binding', function (): void {
            $builder = Builder::query();
            $builder->addBinding(1);

            expect($builder->bindings)->toBe([1]);
        });

        it('adds array of bindings', function (): void {
            $builder = Builder::query();
            $builder->addBinding([1, 2, 3]);

            expect($builder->bindings)->toBe([1, 2, 3]);
        });

        it('ignores null values', function (): void {
            $builder = Builder::query();
            $builder->addBinding(null);

            expect($builder->bindings)->toBe([]);
        });
    });

    describe('clone', function (): void {
        it('creates a copy of the builder', function (): void {
            $builder = Builder::query()->from('users')->where('id', '=', 1);
            $clone = $builder->clone();

            expect($clone)->not->toBe($builder)
                ->and($clone->from)->toBe('users')
                ->and($clone->wheres)->toHaveCount(1);
        });
    });

    describe('cloneWithout', function (): void {
        it('creates a copy without specified properties', function (): void {
            $builder = Builder::query()
                ->from('users')
                ->where('id', '=', 1)
                ->orderBy('name')
                ->limit(10);

            $clone = $builder->cloneWithout(['orders', 'limit']);

            expect($clone->orders)->toBe([])
                ->and($clone->limit)->toBeNull()
                ->and($clone->from)->toBe('users')
                ->and($clone->wheres)->toHaveCount(1);
        });
    });

    describe('tap', function (): void {
        it('executes callback and returns self', function (): void {
            $tapped = false;
            $builder = Builder::query()->tap(function ($query) use (&$tapped): void {
                $tapped = true;
            });

            expect($tapped)->toBeTrue()
                ->and($builder)->toBeInstanceOf(Builder::class);
        });
    });

    describe('getClient', function (): void {
        it('returns null when no client set', function (): void {
            $builder = Builder::query();

            expect($builder->getClient())->toBeNull();
        });
    });

    describe('setClient', function (): void {
        it('sets the client', function (): void {
            $builder = Builder::query();
            $result = $builder->setClient(null);

            expect($result)->toBe($builder);
        });
    });

    describe('getGrammar', function (): void {
        it('returns the grammar instance', function (): void {
            $builder = Builder::query();

            expect($builder->getGrammar())->toBeInstanceOf(Grammar::class);
        });
    });
});

/**
 * CODE REVIEW NOTES for Builder.php:
 *
 * 1. GOOD: Comprehensive fluent interface for building queries
 * 2. GOOD: Support for ClickHouse-specific features (PREWHERE, SAMPLE, FINAL, ARRAY JOIN, LIMIT BY, etc.)
 * 3. GOOD: Proper handling of subqueries via closures
 * 4. GOOD: Conditional building with when/unless
 * 5. GOOD: Clone functionality for query reuse
 *
 * POTENTIAL IMPROVEMENTS:
 * 1. The isOperator method could be a constant array for better performance
 * 2. Consider adding query caching for repeated queries
 * 3. The chunk/lazy methods are well-implemented but could benefit from more documentation
 * 4. Consider adding a debug mode that logs all queries
 * 5. The dd() method uses exit(1) which is not ideal for testing - consider making it configurable
 */
