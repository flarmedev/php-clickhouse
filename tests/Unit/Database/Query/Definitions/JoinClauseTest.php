<?php

declare(strict_types=1);

use Flarme\PhpClickhouse\Database\Query\Builder;
use Flarme\PhpClickhouse\Database\Query\Definitions\JoinClause;
use Flarme\PhpClickhouse\Expressions\Raw;

describe('JoinClause', function () {
    function createJoinClause(string $type = 'INNER', string|Raw $table = 'users'): JoinClause
    {
        $builder = new Builder();

        return new JoinClause($builder, $type, $table);
    }

    describe('construction', function (): void {
        it('stores type and table', function (): void {
            $join = createJoinClause('LEFT', 'orders');

            expect($join->type)->toBe('LEFT')
                ->and($join->table)->toBe('orders');
        });

        it('initializes with empty clauses', function (): void {
            $join = createJoinClause();

            expect($join->clauses)->toBe([])
                ->and($join->alias)->toBeNull()
                ->and($join->using)->toBeNull();
        });

        it('accepts Raw table expression', function (): void {
            $raw = new Raw('(SELECT * FROM users) AS u');
            $join = createJoinClause('INNER', $raw);

            expect($join->table)->toBe($raw);
        });
    });

    describe('from', function (): void {
        it('changes the table', function (): void {
            $join = createJoinClause('INNER', 'users');

            $result = $join->from('orders');

            expect($result)->toBe($join)
                ->and($join->table)->toBe('orders');
        });

        it('accepts Raw expression', function (): void {
            $join = createJoinClause();
            $raw = new Raw('system.tables');

            $join->from($raw);

            expect($join->table)->toBe($raw);
        });
    });

    describe('as', function (): void {
        it('sets the alias', function (): void {
            $join = createJoinClause();

            $result = $join->as('u');

            expect($result)->toBe($join)
                ->and($join->alias)->toBe('u');
        });
    });

    describe('on', function (): void {
        it('adds a basic on clause', function (): void {
            $join = createJoinClause();

            $result = $join->on('users.id', '=', 'orders.user_id');

            expect($result)->toBe($join)
                ->and($join->clauses)->toHaveCount(1)
                ->and($join->clauses[0])->toBe([
                    'type' => 'column',
                    'first' => 'users.id',
                    'operator' => '=',
                    'second' => 'orders.user_id',
                    'boolean' => 'AND',
                ]);
        });

        it('adds multiple on clauses', function (): void {
            $join = createJoinClause();

            $join->on('users.id', '=', 'orders.user_id');
            $join->on('users.status', '=', 'orders.status');

            expect($join->clauses)->toHaveCount(2);
        });

        it('accepts Raw expressions', function (): void {
            $join = createJoinClause();
            $raw = new Raw('users.id');

            $join->on($raw, '=', 'orders.user_id');

            expect($join->clauses[0]['first'])->toBe($raw);
        });

        it('handles nested on with closure', function (): void {
            $join = createJoinClause();

            $join->on(function (JoinClause $j): void {
                $j->on('users.id', '=', 'orders.user_id');
                $j->on('users.status', '=', 'orders.status');
            });

            expect($join->clauses)->toHaveCount(1)
                ->and($join->clauses[0]['type'])->toBe('nested');
        });
    });

    describe('orOn', function (): void {
        it('adds on clause with OR boolean', function (): void {
            $join = createJoinClause();

            $join->on('users.id', '=', 'orders.user_id');
            $join->orOn('users.email', '=', 'orders.email');

            expect($join->clauses[1]['boolean'])->toBe('OR');
        });
    });

    describe('using', function (): void {
        it('sets using columns', function (): void {
            $join = createJoinClause();

            $result = $join->using('id', 'status');

            expect($result)->toBe($join)
                ->and($join->using)->toBe(['id', 'status']);
        });

        it('accepts Raw expressions', function (): void {
            $join = createJoinClause();
            $raw = new Raw('id');

            $join->using($raw, 'status');

            expect($join->using[0])->toBe($raw);
        });
    });

    describe('global', function (): void {
        it('prepends GLOBAL to type', function (): void {
            $join = createJoinClause('INNER', 'users');

            $result = $join->global();

            expect($result)->toBe($join)
                ->and($join->type)->toBe('GLOBAL INNER');
        });

        it('does not duplicate GLOBAL prefix', function (): void {
            $join = createJoinClause('GLOBAL INNER', 'users');

            $join->global();

            expect($join->type)->toBe('GLOBAL INNER');
        });
    });

    describe('strictness modifiers', function (): void {
        describe('any', function (): void {
            it('adds ANY strictness', function (): void {
                $join = createJoinClause('INNER', 'users');

                $result = $join->any();

                expect($result)->toBe($join)
                    ->and($join->type)->toBe('ANY INNER');
            });
        });

        describe('all', function (): void {
            it('adds ALL strictness', function (): void {
                $join = createJoinClause('LEFT', 'users');

                $join->all();

                expect($join->type)->toBe('ALL LEFT');
            });
        });

        describe('semi', function (): void {
            it('adds SEMI strictness', function (): void {
                $join = createJoinClause('INNER', 'users');

                $join->semi();

                expect($join->type)->toBe('SEMI INNER');
            });
        });

        describe('anti', function (): void {
            it('adds ANTI strictness', function (): void {
                $join = createJoinClause('LEFT', 'users');

                $join->anti();

                expect($join->type)->toBe('ANTI LEFT');
            });
        });

        describe('asof', function (): void {
            it('adds ASOF strictness', function (): void {
                $join = createJoinClause('INNER', 'users');

                $join->asof();

                expect($join->type)->toBe('ASOF INNER');
            });
        });
    });

    describe('combined modifiers', function (): void {
        it('supports global with strictness', function (): void {
            $join = createJoinClause('LEFT', 'users');

            $join->global()->any();

            expect($join->type)->toBe('GLOBAL ANY LEFT');
        });
    });

    describe('fluent interface', function (): void {
        it('supports method chaining', function (): void {
            $join = createJoinClause('INNER', 'users');

            $result = $join
                ->as('u')
                ->on('u.id', '=', 'orders.user_id')
                ->global()
                ->any();

            expect($result)->toBe($join)
                ->and($join->alias)->toBe('u')
                ->and($join->clauses)->toHaveCount(1)
                ->and($join->type)->toContain('GLOBAL')
                ->and($join->type)->toContain('ANY');
        });
    });
});

/**
 * CODE REVIEW NOTES for JoinClause.php:
 *
 * 1. GOOD: Comprehensive support for ClickHouse-specific join types (GLOBAL, ANY, ALL, SEMI, ANTI, ASOF)
 * 2. GOOD: Supports nested ON conditions via closures
 * 3. GOOD: Fluent interface design
 * 4. GOOD: Supports both USING and ON syntax
 *
 * POTENTIAL IMPROVEMENTS:
 * 1. The addStrictness method logic is complex - consider simplifying or adding more comments
 * 2. Consider adding validation for valid join types
 * 3. The global() method check uses str_starts_with which is good for PHP 8+
 */
