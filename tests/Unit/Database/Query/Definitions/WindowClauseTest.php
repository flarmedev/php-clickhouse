<?php

declare(strict_types=1);

use Flarme\PhpClickhouse\Database\Query\Definitions\WindowClause;
use Flarme\PhpClickhouse\Expressions\Raw;

describe('WindowClause', function (): void {
    describe('constants', function (): void {
        it('defines UNBOUNDED_PRECEDING', function (): void {
            expect(WindowClause::UNBOUNDED_PRECEDING)->toBe('UNBOUNDED PRECEDING');
        });

        it('defines CURRENT_ROW', function (): void {
            expect(WindowClause::CURRENT_ROW)->toBe('CURRENT ROW');
        });

        it('defines UNBOUNDED_FOLLOWING', function (): void {
            expect(WindowClause::UNBOUNDED_FOLLOWING)->toBe('UNBOUNDED FOLLOWING');
        });
    });

    describe('construction', function (): void {
        it('initializes with empty arrays', function (): void {
            $window = new WindowClause();

            expect($window->partitions)->toBe([])
                ->and($window->orders)->toBe([])
                ->and($window->frame)->toBeNull();
        });
    });

    describe('partitionBy', function (): void {
        it('adds a single partition column', function (): void {
            $window = new WindowClause();

            $result = $window->partitionBy('user_id');

            expect($result)->toBe($window)
                ->and($window->partitions)->toBe(['user_id']);
        });

        it('adds multiple partition columns', function (): void {
            $window = new WindowClause();

            $window->partitionBy('user_id', 'category');

            expect($window->partitions)->toBe(['user_id', 'category']);
        });

        it('can be called multiple times to add more columns', function (): void {
            $window = new WindowClause();

            $window->partitionBy('user_id');
            $window->partitionBy('category');

            expect($window->partitions)->toBe(['user_id', 'category']);
        });

        it('accepts Raw expressions', function (): void {
            $window = new WindowClause();
            $raw = new Raw('toDate(created_at)');

            $window->partitionBy($raw);

            expect($window->partitions[0])->toBe($raw);
        });
    });

    describe('orderBy', function (): void {
        it('adds order with default ASC direction', function (): void {
            $window = new WindowClause();

            $result = $window->orderBy('created_at');

            expect($result)->toBe($window)
                ->and($window->orders)->toBe([
                    ['column' => 'created_at', 'direction' => 'ASC'],
                ]);
        });

        it('adds order with specified direction', function (): void {
            $window = new WindowClause();

            $window->orderBy('created_at', 'DESC');

            expect($window->orders)->toBe([
                ['column' => 'created_at', 'direction' => 'DESC'],
            ]);
        });

        it('normalizes direction to uppercase', function (): void {
            $window = new WindowClause();

            $window->orderBy('created_at', 'desc');

            expect($window->orders[0]['direction'])->toBe('DESC');
        });

        it('can add multiple orders', function (): void {
            $window = new WindowClause();

            $window->orderBy('user_id', 'ASC');
            $window->orderBy('created_at', 'DESC');

            expect($window->orders)->toHaveCount(2)
                ->and($window->orders[0]['column'])->toBe('user_id')
                ->and($window->orders[1]['column'])->toBe('created_at');
        });

        it('accepts Raw expressions', function (): void {
            $window = new WindowClause();
            $raw = new Raw('toDate(created_at)');

            $window->orderBy($raw);

            expect($window->orders[0]['column'])->toBe($raw);
        });
    });

    describe('orderByDesc', function (): void {
        it('adds order with DESC direction', function (): void {
            $window = new WindowClause();

            $result = $window->orderByDesc('created_at');

            expect($result)->toBe($window)
                ->and($window->orders)->toBe([
                    ['column' => 'created_at', 'direction' => 'DESC'],
                ]);
        });

        it('accepts Raw expressions', function (): void {
            $window = new WindowClause();
            $raw = new Raw('amount');

            $window->orderByDesc($raw);

            expect($window->orders[0]['column'])->toBe($raw)
                ->and($window->orders[0]['direction'])->toBe('DESC');
        });
    });

    describe('rows', function (): void {
        it('sets ROWS frame', function (): void {
            $window = new WindowClause();

            $result = $window->rows('UNBOUNDED PRECEDING', 'CURRENT ROW');

            expect($result)->toBe($window)
                ->and($window->frame)->toBe([
                    'type' => 'ROWS',
                    'start' => 'UNBOUNDED PRECEDING',
                    'end' => 'CURRENT ROW',
                ]);
        });

        it('handles numeric bounds', function (): void {
            $window = new WindowClause();

            $window->rows('1 PRECEDING', '1 FOLLOWING');

            expect($window->frame)->toBe([
                'type' => 'ROWS',
                'start' => '1 PRECEDING',
                'end' => '1 FOLLOWING',
            ]);
        });
    });

    describe('range', function (): void {
        it('sets RANGE frame', function (): void {
            $window = new WindowClause();

            $result = $window->range('UNBOUNDED PRECEDING', 'CURRENT ROW');

            expect($result)->toBe($window)
                ->and($window->frame)->toBe([
                    'type' => 'RANGE',
                    'start' => 'UNBOUNDED PRECEDING',
                    'end' => 'CURRENT ROW',
                ]);
        });
    });

    describe('rowsBetween', function (): void {
        it('is an alias for rows', function (): void {
            $window = new WindowClause();

            $result = $window->rowsBetween('UNBOUNDED PRECEDING', 'UNBOUNDED FOLLOWING');

            expect($result)->toBe($window)
                ->and($window->frame['type'])->toBe('ROWS');
        });
    });

    describe('rangeBetween', function (): void {
        it('is an alias for range', function (): void {
            $window = new WindowClause();

            $result = $window->rangeBetween('UNBOUNDED PRECEDING', 'UNBOUNDED FOLLOWING');

            expect($result)->toBe($window)
                ->and($window->frame['type'])->toBe('RANGE');
        });
    });

    describe('fluent interface', function (): void {
        it('supports method chaining', function (): void {
            $window = new WindowClause();

            $result = $window
                ->partitionBy('user_id')
                ->orderBy('created_at', 'DESC')
                ->rows('UNBOUNDED PRECEDING', 'CURRENT ROW');

            expect($result)->toBe($window)
                ->and($window->partitions)->toBe(['user_id'])
                ->and($window->orders)->toHaveCount(1)
                ->and($window->frame)->not->toBeNull();
        });
    });
});

/**
 * CODE REVIEW NOTES for WindowClause.php:
 *
 * 1. GOOD: Clean fluent interface design
 * 2. GOOD: Supports both string columns and Raw expressions
 * 3. GOOD: Direction normalization to uppercase
 * 4. GOOD: Alias methods (rowsBetween, rangeBetween) for readability
 *
 * POTENTIAL IMPROVEMENTS:
 * 1. Consider validating frame bounds (e.g., valid SQL frame specifications)
 * 2. Could add constants for common frame bounds like UNBOUNDED_PRECEDING, CURRENT_ROW
 * 3. The strtoupper() in orderBy could be mb_strtoupper() for consistency with rest of codebase
 */
