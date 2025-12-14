<?php

declare(strict_types=1);

use Flarme\PhpClickhouse\Database\Schema\Components\Column;

describe('Column', function (): void {
    describe('construction', function (): void {
        it('stores name and type', function (): void {
            $column = new Column('id', 'UInt64');

            expect($column->getName())->toBe('id')
                ->and($column->getType())->toBe('UInt64');
        });

        it('initializes with default values', function (): void {
            $column = new Column('id', 'UInt64');

            expect($column->isNullable())->toBeFalse()
                ->and($column->hasDefault())->toBeFalse()
                ->and($column->getDefault())->toBeNull()
                ->and($column->getDefaultExpression())->toBeNull()
                ->and($column->getMaterialized())->toBeNull()
                ->and($column->getAlias())->toBeNull()
                ->and($column->getCodec())->toBeNull()
                ->and($column->getTtl())->toBeNull()
                ->and($column->getComment())->toBeNull()
                ->and($column->getAfter())->toBeNull()
                ->and($column->isFirst())->toBeFalse();
        });
    });

    describe('nullable', function (): void {
        it('sets nullable flag', function (): void {
            $column = new Column('name', 'String');

            $result = $column->nullable();

            expect($result)->toBe($column)
                ->and($column->isNullable())->toBeTrue();
        });

        it('can disable nullable', function (): void {
            $column = new Column('name', 'String');

            $column->nullable()->nullable(false);

            expect($column->isNullable())->toBeFalse();
        });
    });

    describe('default', function (): void {
        it('sets default value', function (): void {
            $column = new Column('status', 'String');

            $result = $column->default('active');

            expect($result)->toBe($column)
                ->and($column->hasDefault())->toBeTrue()
                ->and($column->getDefault())->toBe('active');
        });

        it('handles null default', function (): void {
            $column = new Column('deleted_at', 'DateTime');

            $column->default(null);

            expect($column->hasDefault())->toBeTrue()
                ->and($column->getDefault())->toBeNull();
        });

        it('handles numeric default', function (): void {
            $column = new Column('count', 'UInt32');

            $column->default(0);

            expect($column->getDefault())->toBe(0);
        });

        it('handles boolean default', function (): void {
            $column = new Column('active', 'Bool');

            $column->default(true);

            expect($column->getDefault())->toBeTrue();
        });
    });

    describe('defaultExpression', function (): void {
        it('sets default expression', function (): void {
            $column = new Column('created_at', 'DateTime');

            $result = $column->defaultExpression('now()');

            expect($result)->toBe($column)
                ->and($column->getDefaultExpression())->toBe('now()');
        });

        it('handles complex expressions', function (): void {
            $column = new Column('uuid', 'UUID');

            $column->defaultExpression('generateUUIDv4()');

            expect($column->getDefaultExpression())->toBe('generateUUIDv4()');
        });
    });

    describe('materialized', function (): void {
        it('sets materialized expression', function (): void {
            $column = new Column('year', 'UInt16');

            $result = $column->materialized('toYear(created_at)');

            expect($result)->toBe($column)
                ->and($column->getMaterialized())->toBe('toYear(created_at)');
        });
    });

    describe('alias', function (): void {
        it('sets alias expression', function (): void {
            $column = new Column('full_name', 'String');

            $result = $column->alias("concat(first_name, ' ', last_name)");

            expect($result)->toBe($column)
                ->and($column->getAlias())->toBe("concat(first_name, ' ', last_name)");
        });
    });

    describe('codec', function (): void {
        it('sets compression codec', function (): void {
            $column = new Column('data', 'String');

            $result = $column->codec('ZSTD(3)');

            expect($result)->toBe($column)
                ->and($column->getCodec())->toBe('ZSTD(3)');
        });

        it('handles multiple codecs', function (): void {
            $column = new Column('data', 'String');

            $column->codec('Delta, ZSTD');

            expect($column->getCodec())->toBe('Delta, ZSTD');
        });
    });

    describe('ttl', function (): void {
        it('sets TTL expression', function (): void {
            $column = new Column('temp_data', 'String');

            $result = $column->ttl('created_at + INTERVAL 1 DAY');

            expect($result)->toBe($column)
                ->and($column->getTtl())->toBe('created_at + INTERVAL 1 DAY');
        });
    });

    describe('comment', function (): void {
        it('sets comment', function (): void {
            $column = new Column('id', 'UInt64');

            $result = $column->comment('Primary key');

            expect($result)->toBe($column)
                ->and($column->getComment())->toBe('Primary key');
        });

        it('handles special characters in comment', function (): void {
            $column = new Column('data', 'String');

            $column->comment("User's data with \"quotes\"");

            expect($column->getComment())->toBe("User's data with \"quotes\"");
        });
    });

    describe('after', function (): void {
        it('sets after column for ALTER', function (): void {
            $column = new Column('new_column', 'String');

            $result = $column->after('existing_column');

            expect($result)->toBe($column)
                ->and($column->getAfter())->toBe('existing_column')
                ->and($column->isFirst())->toBeFalse();
        });

        it('clears first flag when setting after', function (): void {
            $column = new Column('new_column', 'String');

            $column->first()->after('existing_column');

            expect($column->isFirst())->toBeFalse()
                ->and($column->getAfter())->toBe('existing_column');
        });
    });

    describe('first', function (): void {
        it('sets first flag for ALTER', function (): void {
            $column = new Column('new_column', 'String');

            $result = $column->first();

            expect($result)->toBe($column)
                ->and($column->isFirst())->toBeTrue()
                ->and($column->getAfter())->toBeNull();
        });

        it('clears after when setting first', function (): void {
            $column = new Column('new_column', 'String');

            $column->after('existing_column')->first();

            expect($column->isFirst())->toBeTrue()
                ->and($column->getAfter())->toBeNull();
        });
    });

    describe('fluent interface', function (): void {
        it('supports method chaining', function (): void {
            $column = new Column('data', 'String');

            $result = $column
                ->nullable()
                ->default('')
                ->codec('ZSTD')
                ->comment('Data field');

            expect($result)->toBe($column)
                ->and($column->isNullable())->toBeTrue()
                ->and($column->hasDefault())->toBeTrue()
                ->and($column->getCodec())->toBe('ZSTD')
                ->and($column->getComment())->toBe('Data field');
        });
    });
});

/**
 * CODE REVIEW NOTES for Column.php:
 *
 * 1. GOOD: Clean fluent interface design
 * 2. GOOD: Proper separation of default value vs default expression
 * 3. GOOD: Support for ClickHouse-specific features (MATERIALIZED, ALIAS, CODEC, TTL)
 * 4. GOOD: Mutual exclusivity of first() and after() is properly handled
 *
 * POTENTIAL IMPROVEMENTS:
 * 1. Consider adding validation for codec names
 * 2. Could add type hints for common ClickHouse types as constants
 * 3. The hasDefault flag is separate from default value - this is correct but could be documented better
 */
