<?php

declare(strict_types=1);

use Flarme\PhpClickhouse\Database\Query\Builder;
use Flarme\PhpClickhouse\Database\Query\Grammar;
use Flarme\PhpClickhouse\Expressions\Raw;

describe('Grammar', function () {
    function createQueryGrammar(): Grammar
    {
        return new Grammar();
    }

    describe('compileSelect', function (): void {
        it('compiles basic select all', function (): void {
            $builder = Builder::query()->from('users');
            $grammar = createQueryGrammar();

            $sql = $grammar->compileSelect($builder);

            expect($sql)->toBe('SELECT * FROM `users`');
        });

        it('compiles select with specific columns', function (): void {
            $builder = Builder::query()->select('id', 'name')->from('users');
            $grammar = createQueryGrammar();

            $sql = $grammar->compileSelect($builder);

            expect($sql)->toBe('SELECT `id`, `name` FROM `users`');
        });

        it('compiles select distinct', function (): void {
            $builder = Builder::query()->select('name')->distinct()->from('users');
            $grammar = createQueryGrammar();

            $sql = $grammar->compileSelect($builder);

            expect($sql)->toBe('SELECT DISTINCT `name` FROM `users`');
        });

        it('compiles select with table alias', function (): void {
            $builder = Builder::query()->from('users', 'u');
            $grammar = createQueryGrammar();

            $sql = $grammar->compileSelect($builder);

            expect($sql)->toBe('SELECT * FROM `users` AS `u`');
        });

        it('compiles select with FINAL', function (): void {
            $builder = Builder::query()->from('users')->final();
            $grammar = createQueryGrammar();

            $sql = $grammar->compileSelect($builder);

            expect($sql)->toBe('SELECT * FROM `users` FINAL');
        });

        it('compiles select with SAMPLE', function (): void {
            $builder = Builder::query()->from('users')->sample(0.1);
            $grammar = createQueryGrammar();

            $sql = $grammar->compileSelect($builder);

            expect($sql)->toBe('SELECT * FROM `users` SAMPLE 0.1');
        });

        it('compiles select with SAMPLE and OFFSET', function (): void {
            $builder = Builder::query()->from('users')->sample(0.1, 0.5);
            $grammar = createQueryGrammar();

            $sql = $grammar->compileSelect($builder);

            expect($sql)->toBe('SELECT * FROM `users` SAMPLE 0.1 OFFSET 0.5');
        });
    });

    describe('wrap', function (): void {
        it('wraps column names with backticks', function (): void {
            $grammar = createQueryGrammar();

            expect($grammar->wrap('column'))->toBe('`column`');
        });

        it('does not wrap asterisk', function (): void {
            $grammar = createQueryGrammar();

            expect($grammar->wrap('*'))->toBe('*');
        });

        it('wraps aliased columns', function (): void {
            $grammar = createQueryGrammar();

            expect($grammar->wrap('column as alias'))->toBe('`column` AS `alias`');
        });

        it('wraps aliased columns with uppercase AS', function (): void {
            $grammar = createQueryGrammar();

            expect($grammar->wrap('column AS alias'))->toBe('`column` AS `alias`');
        });

        it('wraps dotted column names', function (): void {
            $grammar = createQueryGrammar();

            expect($grammar->wrap('table.column'))->toBe('`table`.`column`');
        });

        it('returns Raw expression as-is', function (): void {
            $grammar = createQueryGrammar();
            $raw = new Raw('COUNT(*)');

            expect($grammar->wrap($raw))->toBe('COUNT(*)');
        });

        it('escapes backticks in column names', function (): void {
            $grammar = createQueryGrammar();

            expect($grammar->wrap('col`umn'))->toBe('`col``umn`');
        });
    });

    describe('compileWheres', function (): void {
        it('compiles basic where', function (): void {
            $builder = Builder::query()->from('users')->where('id', '=', 1);
            $grammar = createQueryGrammar();

            $sql = $grammar->compileSelect($builder);

            expect($sql)->toContain('WHERE `id` = ?');
        });

        it('compiles multiple wheres with AND', function (): void {
            $builder = Builder::query()->from('users')
                ->where('id', '=', 1)
                ->where('status', '=', 'active');
            $grammar = createQueryGrammar();

            $sql = $grammar->compileSelect($builder);

            expect($sql)->toContain('WHERE `id` = ? AND `status` = ?');
        });

        it('compiles where with OR', function (): void {
            $builder = Builder::query()->from('users')
                ->where('id', '=', 1)
                ->orWhere('id', '=', 2);
            $grammar = createQueryGrammar();

            $sql = $grammar->compileSelect($builder);

            expect($sql)->toContain('WHERE `id` = ? OR `id` = ?');
        });

        it('compiles whereIn', function (): void {
            $builder = Builder::query()->from('users')->whereIn('id', [1, 2, 3]);
            $grammar = createQueryGrammar();

            $sql = $grammar->compileSelect($builder);

            expect($sql)->toContain('WHERE `id` IN (?, ?, ?)');
        });

        it('compiles whereNotIn', function (): void {
            $builder = Builder::query()->from('users')->whereNotIn('id', [1, 2]);
            $grammar = createQueryGrammar();

            $sql = $grammar->compileSelect($builder);

            expect($sql)->toContain('WHERE `id` NOT IN (?, ?)');
        });

        it('compiles whereIn with subquery', function (): void {
            $builder = Builder::query()->from('users')
                ->whereIn('id', function ($query): void {
                    $query->select('user_id')->from('orders');
                });
            $grammar = createQueryGrammar();

            $sql = $grammar->compileSelect($builder);

            expect($sql)->toContain('WHERE `id` IN (SELECT `user_id` FROM `orders`)');
        });

        it('compiles whereGlobalIn', function (): void {
            $builder = Builder::query()->from('users')->whereGlobalIn('id', [1, 2]);
            $grammar = createQueryGrammar();

            $sql = $grammar->compileSelect($builder);

            expect($sql)->toContain('WHERE `id` GLOBAL IN (?, ?)');
        });

        it('compiles whereGlobalNotIn', function (): void {
            $builder = Builder::query()->from('users')->whereGlobalNotIn('id', [1, 2]);
            $grammar = createQueryGrammar();

            $sql = $grammar->compileSelect($builder);

            expect($sql)->toContain('WHERE `id` GLOBAL NOT IN (?, ?)');
        });

        it('compiles whereBetween', function (): void {
            $builder = Builder::query()->from('users')->whereBetween('age', [18, 65]);
            $grammar = createQueryGrammar();

            $sql = $grammar->compileSelect($builder);

            expect($sql)->toContain('WHERE `age` BETWEEN ? AND ?');
        });

        it('compiles whereNotBetween', function (): void {
            $builder = Builder::query()->from('users')->whereNotBetween('age', [0, 17]);
            $grammar = createQueryGrammar();

            $sql = $grammar->compileSelect($builder);

            expect($sql)->toContain('WHERE `age` NOT BETWEEN ? AND ?');
        });

        it('compiles whereNull', function (): void {
            $builder = Builder::query()->from('users')->whereNull('deleted_at');
            $grammar = createQueryGrammar();

            $sql = $grammar->compileSelect($builder);

            expect($sql)->toContain('WHERE `deleted_at` IS NULL');
        });

        it('compiles whereNotNull', function (): void {
            $builder = Builder::query()->from('users')->whereNotNull('email');
            $grammar = createQueryGrammar();

            $sql = $grammar->compileSelect($builder);

            expect($sql)->toContain('WHERE `email` IS NOT NULL');
        });

        it('compiles whereColumn', function (): void {
            $builder = Builder::query()->from('users')->whereColumn('created_at', '=', 'updated_at');
            $grammar = createQueryGrammar();

            $sql = $grammar->compileSelect($builder);

            expect($sql)->toContain('WHERE `created_at` = `updated_at`');
        });

        it('compiles whereExists', function (): void {
            $builder = Builder::query()->from('users')
                ->whereExists(function ($query): void {
                    $query->select('id')->from('orders')->where('orders.user_id', '=', 'users.id');
                });
            $grammar = createQueryGrammar();

            $sql = $grammar->compileSelect($builder);

            expect($sql)->toContain('WHERE EXISTS (SELECT `id` FROM `orders`');
        });

        it('compiles whereNotExists', function (): void {
            $builder = Builder::query()->from('users')
                ->whereNotExists(function ($query): void {
                    $query->select('id')->from('orders');
                });
            $grammar = createQueryGrammar();

            $sql = $grammar->compileSelect($builder);

            expect($sql)->toContain('WHERE NOT EXISTS');
        });

        it('compiles whereRaw', function (): void {
            $builder = Builder::query()->from('users')->whereRaw('id > 10');
            $grammar = createQueryGrammar();

            $sql = $grammar->compileSelect($builder);

            expect($sql)->toContain('WHERE id > 10');
        });

        it('compiles nested where', function (): void {
            $builder = Builder::query()->from('users')
                ->where('status', '=', 'active')
                ->where(function ($query): void {
                    $query->where('role', '=', 'admin')
                        ->orWhere('role', '=', 'moderator');
                });
            $grammar = createQueryGrammar();

            $sql = $grammar->compileSelect($builder);

            expect($sql)->toContain('WHERE `status` = ? AND (`role` = ? OR `role` = ?)');
        });
    });

    describe('compilePreWheres', function (): void {
        it('compiles PREWHERE clause', function (): void {
            $builder = Builder::query()->from('users')->preWhere('date', '=', '2024-01-01');
            $grammar = createQueryGrammar();

            $sql = $grammar->compileSelect($builder);

            expect($sql)->toContain('PREWHERE `date` = ?');
        });
    });

    describe('compileJoins', function (): void {
        it('compiles INNER JOIN', function (): void {
            $builder = Builder::query()->from('users')
                ->join('orders', 'users.id', '=', 'orders.user_id');
            $grammar = createQueryGrammar();

            $sql = $grammar->compileSelect($builder);

            expect($sql)->toContain('INNER JOIN `orders` ON `users`.`id` = `orders`.`user_id`');
        });

        it('compiles LEFT JOIN', function (): void {
            $builder = Builder::query()->from('users')
                ->leftJoin('orders', 'users.id', '=', 'orders.user_id');
            $grammar = createQueryGrammar();

            $sql = $grammar->compileSelect($builder);

            expect($sql)->toContain('LEFT JOIN `orders`');
        });

        it('compiles RIGHT JOIN', function (): void {
            $builder = Builder::query()->from('users')
                ->rightJoin('orders', 'users.id', '=', 'orders.user_id');
            $grammar = createQueryGrammar();

            $sql = $grammar->compileSelect($builder);

            expect($sql)->toContain('RIGHT JOIN `orders`');
        });

        it('compiles CROSS JOIN', function (): void {
            $builder = Builder::query()->from('users')->crossJoin('categories');
            $grammar = createQueryGrammar();

            $sql = $grammar->compileSelect($builder);

            expect($sql)->toContain('CROSS JOIN `categories`');
        });

        it('compiles join with alias', function (): void {
            $builder = Builder::query()->from('users')
                ->join('orders', function ($join): void {
                    $join->as('o')->on('users.id', '=', 'o.user_id');
                });
            $grammar = createQueryGrammar();

            $sql = $grammar->compileSelect($builder);

            expect($sql)->toContain('`orders` AS `o`');
        });

        it('compiles join with USING', function (): void {
            $builder = Builder::query()->from('users')
                ->join('orders', function ($join): void {
                    $join->using('user_id');
                });
            $grammar = createQueryGrammar();

            $sql = $grammar->compileSelect($builder);

            expect($sql)->toContain('USING (`user_id`)');
        });
    });

    describe('compileArrayJoins', function (): void {
        it('compiles ARRAY JOIN', function (): void {
            $builder = Builder::query()->from('users')->arrayJoin('tags');
            $grammar = createQueryGrammar();

            $sql = $grammar->compileSelect($builder);

            expect($sql)->toContain('ARRAY JOIN `tags`');
        });

        it('compiles LEFT ARRAY JOIN', function (): void {
            $builder = Builder::query()->from('users')->leftArrayJoin('tags');
            $grammar = createQueryGrammar();

            $sql = $grammar->compileSelect($builder);

            expect($sql)->toContain('LEFT ARRAY JOIN `tags`');
        });

        it('compiles ARRAY JOIN with alias', function (): void {
            $builder = Builder::query()->from('users')->arrayJoin('tags', 't');
            $grammar = createQueryGrammar();

            $sql = $grammar->compileSelect($builder);

            expect($sql)->toContain('ARRAY JOIN `tags` AS `t`');
        });
    });

    describe('compileGroups', function (): void {
        it('compiles GROUP BY', function (): void {
            $builder = Builder::query()->from('users')->groupBy('status');
            $grammar = createQueryGrammar();

            $sql = $grammar->compileSelect($builder);

            expect($sql)->toContain('GROUP BY `status`');
        });

        it('compiles GROUP BY with multiple columns', function (): void {
            $builder = Builder::query()->from('users')->groupBy('status', 'role');
            $grammar = createQueryGrammar();

            $sql = $grammar->compileSelect($builder);

            expect($sql)->toContain('GROUP BY `status`, `role`');
        });

        it('compiles GROUP BY WITH ROLLUP', function (): void {
            $builder = Builder::query()->from('users')->groupBy('status')->withRollup();
            $grammar = createQueryGrammar();

            $sql = $grammar->compileSelect($builder);

            expect($sql)->toContain('GROUP BY `status` WITH ROLLUP');
        });

        it('compiles GROUP BY WITH CUBE', function (): void {
            $builder = Builder::query()->from('users')->groupBy('status')->withCube();
            $grammar = createQueryGrammar();

            $sql = $grammar->compileSelect($builder);

            expect($sql)->toContain('GROUP BY `status` WITH CUBE');
        });

        it('compiles GROUP BY WITH TOTALS', function (): void {
            $builder = Builder::query()->from('users')->groupBy('status')->withTotals();
            $grammar = createQueryGrammar();

            $sql = $grammar->compileSelect($builder);

            expect($sql)->toContain('GROUP BY `status` WITH TOTALS');
        });
    });

    describe('compileHavings', function (): void {
        it('compiles HAVING', function (): void {
            $builder = Builder::query()->from('users')
                ->groupBy('status')
                ->having('count', '>', 10);
            $grammar = createQueryGrammar();

            $sql = $grammar->compileSelect($builder);

            expect($sql)->toContain('HAVING `count` > ?');
        });

        it('compiles HAVING with raw', function (): void {
            $builder = Builder::query()->from('users')
                ->groupBy('status')
                ->havingRaw('COUNT(*) > 10');
            $grammar = createQueryGrammar();

            $sql = $grammar->compileSelect($builder);

            expect($sql)->toContain('HAVING COUNT(*) > 10');
        });
    });

    describe('compileOrders', function (): void {
        it('compiles ORDER BY', function (): void {
            $builder = Builder::query()->from('users')->orderBy('name');
            $grammar = createQueryGrammar();

            $sql = $grammar->compileSelect($builder);

            expect($sql)->toContain('ORDER BY `name` ASC');
        });

        it('compiles ORDER BY DESC', function (): void {
            $builder = Builder::query()->from('users')->orderByDesc('created_at');
            $grammar = createQueryGrammar();

            $sql = $grammar->compileSelect($builder);

            expect($sql)->toContain('ORDER BY `created_at` DESC');
        });

        it('compiles multiple ORDER BY', function (): void {
            $builder = Builder::query()->from('users')
                ->orderBy('status')
                ->orderByDesc('created_at');
            $grammar = createQueryGrammar();

            $sql = $grammar->compileSelect($builder);

            expect($sql)->toContain('ORDER BY `status` ASC, `created_at` DESC');
        });

        it('compiles ORDER BY raw', function (): void {
            $builder = Builder::query()->from('users')->orderByRaw('RAND()');
            $grammar = createQueryGrammar();

            $sql = $grammar->compileSelect($builder);

            expect($sql)->toContain('ORDER BY RAND()');
        });
    });

    describe('compileLimit', function (): void {
        it('compiles LIMIT', function (): void {
            $builder = Builder::query()->from('users')->limit(10);
            $grammar = createQueryGrammar();

            $sql = $grammar->compileSelect($builder);

            expect($sql)->toContain('LIMIT 10');
        });
    });

    describe('compileOffset', function (): void {
        it('compiles OFFSET', function (): void {
            $builder = Builder::query()->from('users')->offset(20);
            $grammar = createQueryGrammar();

            $sql = $grammar->compileSelect($builder);

            expect($sql)->toContain('OFFSET 20');
        });
    });

    describe('compileLimitBy', function (): void {
        it('compiles LIMIT BY', function (): void {
            $builder = Builder::query()->from('users')->limitBy(5, 'user_id');
            $grammar = createQueryGrammar();

            $sql = $grammar->compileSelect($builder);

            expect($sql)->toContain('LIMIT 5 BY `user_id`');
        });

        it('compiles LIMIT BY with offset', function (): void {
            $builder = Builder::query()->from('users')->limitByOffset(5, 10, 'user_id');
            $grammar = createQueryGrammar();

            $sql = $grammar->compileSelect($builder);

            expect($sql)->toContain('LIMIT 5 OFFSET 10 BY `user_id`');
        });
    });

    describe('compileUnions', function (): void {
        it('compiles UNION ALL', function (): void {
            $builder = Builder::query()->from('users')
                ->union(function ($query): void {
                    $query->from('admins');
                });
            $grammar = createQueryGrammar();

            $sql = $grammar->compileSelect($builder);

            expect($sql)->toContain('UNION ALL (SELECT * FROM `admins`)');
        });

        it('compiles UNION DISTINCT', function (): void {
            $builder = Builder::query()->from('users')
                ->union(function ($query): void {
                    $query->from('admins');
                }, false);
            $grammar = createQueryGrammar();

            $sql = $grammar->compileSelect($builder);

            expect($sql)->toContain('UNION DISTINCT');
        });
    });

    describe('compileIntersects', function (): void {
        it('compiles INTERSECT', function (): void {
            $builder = Builder::query()->from('users')
                ->intersect(function ($query): void {
                    $query->from('active_users');
                });
            $grammar = createQueryGrammar();

            $sql = $grammar->compileSelect($builder);

            expect($sql)->toContain('INTERSECT (SELECT * FROM `active_users`)');
        });

        it('compiles INTERSECT DISTINCT', function (): void {
            $builder = Builder::query()->from('users')
                ->intersect(function ($query): void {
                    $query->from('active_users');
                }, true);
            $grammar = createQueryGrammar();

            $sql = $grammar->compileSelect($builder);

            expect($sql)->toContain('INTERSECT DISTINCT');
        });
    });

    describe('compileCtes', function (): void {
        it('compiles WITH clause', function (): void {
            $builder = Builder::query()
                ->with('active_users', function ($query): void {
                    $query->from('users')->where('status', '=', 'active');
                })
                ->from('active_users');
            $grammar = createQueryGrammar();

            $sql = $grammar->compileSelect($builder);

            expect($sql)->toStartWith('WITH `active_users` AS (SELECT * FROM `users`');
        });
    });

    describe('compileWindows', function (): void {
        it('compiles WINDOW clause', function (): void {
            $builder = Builder::query()->from('users')
                ->window('w', function ($window): void {
                    $window->partitionBy('user_id')->orderBy('created_at');
                });
            $grammar = createQueryGrammar();

            $sql = $grammar->compileSelect($builder);

            expect($sql)->toContain('WINDOW `w` AS (PARTITION BY `user_id` ORDER BY `created_at` ASC)');
        });
    });

    describe('compileQualifies', function (): void {
        it('compiles QUALIFY clause', function (): void {
            $builder = Builder::query()->from('users')->qualify('row_num', '=', 1);
            $grammar = createQueryGrammar();

            $sql = $grammar->compileSelect($builder);

            expect($sql)->toContain('QUALIFY `row_num` = ?');
        });
    });
});

/**
 * CODE REVIEW NOTES for Grammar.php:
 *
 * 1. GOOD: Comprehensive support for ClickHouse-specific SQL features (PREWHERE, SAMPLE, FINAL, ARRAY JOIN, etc.)
 * 2. GOOD: Clean separation of compilation methods for each SQL component
 * 3. GOOD: Proper escaping of identifiers with backticks
 * 4. GOOD: Support for Raw expressions throughout
 *
 * POTENTIAL IMPROVEMENTS:
 * 1. The compileCondition method uses a match expression which is clean but could benefit from extracting each case to a method
 * 2. Consider adding validation for SQL injection in raw expressions (though Raw is intentionally... raw)
 * 3. The wrapValue method handles aliased values but the regex split could be more robust
 * 4. Consider caching compiled SQL for repeated queries
 */
