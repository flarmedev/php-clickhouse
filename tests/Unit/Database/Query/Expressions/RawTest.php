<?php

declare(strict_types=1);

use Flarme\PhpClickhouse\Contracts\ExpressionInterface;
use Flarme\PhpClickhouse\Expressions\Raw;

describe('Raw', function (): void {
    describe('construction', function (): void {
        it('implements ExpressionInterface', function (): void {
            $raw = new Raw('COUNT(*)');

            expect($raw)->toBeInstanceOf(ExpressionInterface::class);
        });

        it('stores the expression', function (): void {
            $raw = new Raw('COUNT(*)');

            expect($raw->expression)->toBe('COUNT(*)');
        });

        it('handles empty expression', function (): void {
            $raw = new Raw('');

            expect($raw->expression)->toBe('');
        });

        it('handles complex expressions', function (): void {
            $expression = "CASE WHEN status = 'active' THEN 1 ELSE 0 END";
            $raw = new Raw($expression);

            expect($raw->expression)->toBe($expression);
        });
    });

    describe('__toString', function (): void {
        it('returns the expression as string', function (): void {
            $raw = new Raw('SUM(amount)');

            expect((string) $raw)->toBe('SUM(amount)');
        });

        it('can be used in string context', function (): void {
            $raw = new Raw('NOW()');

            expect("SELECT {$raw}")->toBe('SELECT NOW()');
        });

        it('handles special characters', function (): void {
            $raw = new Raw("toDateTime('2024-01-01 00:00:00')");

            expect((string) $raw)->toBe("toDateTime('2024-01-01 00:00:00')");
        });
    });

    describe('expression property', function (): void {
        it('is publicly readable via private(set)', function (): void {
            $raw = new Raw('test');

            // The property should be readable
            expect($raw->expression)->toBe('test');
        });
    });

    describe('make', function (): void {
        it('creates a new Raw instance', function (): void {
            $raw = Raw::make('COUNT(*)');

            expect($raw)->toBeInstanceOf(Raw::class)
                ->and($raw->expression)->toBe('COUNT(*)');
        });

        it('is equivalent to constructor', function (): void {
            $expression = 'SUM(amount)';
            $raw1 = new Raw($expression);
            $raw2 = Raw::make($expression);

            expect($raw1->expression)->toBe($raw2->expression);
        });
    });
});

/**
 * CODE REVIEW NOTES for Raw.php:
 *
 * 1. GOOD: Uses PHP 8.4's private(set) visibility modifier for immutability
 * 2. GOOD: Implements ExpressionInterface for type safety
 * 3. GOOD: Simple and focused - does one thing well
 * 4. GOOD: Implements __toString for easy string interpolation
 *
 * POTENTIAL IMPROVEMENTS:
 * 1. Consider adding a static factory method for consistency with other classes
 * 2. Could add validation to prevent SQL injection in certain contexts (though Raw is meant to be... raw)
 */
