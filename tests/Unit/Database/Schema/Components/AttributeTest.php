<?php

declare(strict_types=1);

use Flarme\PhpClickhouse\Database\Schema\Components\Attribute;

describe('Attribute', function (): void {
    describe('construction', function (): void {
        it('stores name and type', function (): void {
            $attribute = new Attribute('name', 'String');

            expect($attribute->getName())->toBe('name')
                ->and($attribute->getType())->toBe('String');
        });

        it('initializes without default', function (): void {
            $attribute = new Attribute('name', 'String');

            expect($attribute->hasDefault())->toBeFalse()
                ->and($attribute->getDefault())->toBeNull();
        });

        it('can be constructed with default value', function (): void {
            $attribute = new Attribute('status', 'String', 'unknown');

            expect($attribute->hasDefault())->toBeTrue()
                ->and($attribute->getDefault())->toBe('unknown');
        });

        it('handles null default value', function (): void {
            $attribute = new Attribute('optional', 'String', null);

            expect($attribute->hasDefault())->toBeTrue()
                ->and($attribute->getDefault())->toBeNull();
        });

        it('handles numeric default value', function (): void {
            $attribute = new Attribute('count', 'UInt64', 0);

            expect($attribute->hasDefault())->toBeTrue()
                ->and($attribute->getDefault())->toBe(0);
        });

        it('initializes with default flags', function (): void {
            $attribute = new Attribute('name', 'String');

            expect($attribute->isHierarchical())->toBeFalse()
                ->and($attribute->isInjective())->toBeFalse()
                ->and($attribute->isExpression())->toBeFalse()
                ->and($attribute->getExpression())->toBeNull();
        });
    });

    describe('hierarchical', function (): void {
        it('sets hierarchical flag', function (): void {
            $attribute = new Attribute('parent_id', 'UInt64');

            $result = $attribute->hierarchical();

            expect($result)->toBe($attribute)
                ->and($attribute->isHierarchical())->toBeTrue();
        });

        it('can disable hierarchical', function (): void {
            $attribute = new Attribute('parent_id', 'UInt64');

            $attribute->hierarchical()->hierarchical(false);

            expect($attribute->isHierarchical())->toBeFalse();
        });
    });

    describe('injective', function (): void {
        it('sets injective flag', function (): void {
            $attribute = new Attribute('code', 'String');

            $result = $attribute->injective();

            expect($result)->toBe($attribute)
                ->and($attribute->isInjective())->toBeTrue();
        });

        it('can disable injective', function (): void {
            $attribute = new Attribute('code', 'String');

            $attribute->injective()->injective(false);

            expect($attribute->isInjective())->toBeFalse();
        });
    });

    describe('expression', function (): void {
        it('sets expression', function (): void {
            $attribute = new Attribute('computed', 'UInt64');

            $result = $attribute->expression('id * 2');

            expect($result)->toBe($attribute)
                ->and($attribute->isExpression())->toBeTrue()
                ->and($attribute->getExpression())->toBe('id * 2');
        });

        it('handles complex expressions', function (): void {
            $attribute = new Attribute('full_name', 'String');

            $attribute->expression("concat(first_name, ' ', last_name)");

            expect($attribute->getExpression())->toBe("concat(first_name, ' ', last_name)");
        });
    });

    describe('fluent interface', function (): void {
        it('supports method chaining', function (): void {
            $attribute = new Attribute('parent_id', 'UInt64', 0);

            $result = $attribute
                ->hierarchical()
                ->injective();

            expect($result)->toBe($attribute)
                ->and($attribute->isHierarchical())->toBeTrue()
                ->and($attribute->isInjective())->toBeTrue()
                ->and($attribute->hasDefault())->toBeTrue();
        });
    });
});

/**
 * CODE REVIEW NOTES for Attribute.php:
 *
 * 1. GOOD: Clean constructor with optional default parameter using func_num_args()
 * 2. GOOD: Support for dictionary-specific features (hierarchical, injective, expression)
 * 3. GOOD: Fluent interface design
 *
 * POTENTIAL IMPROVEMENTS:
 * 1. Consider adding validation for expression syntax
 * 2. Could add documentation about when to use hierarchical vs injective
 * 3. The func_num_args() check is clever but could be replaced with a more explicit approach
 */
