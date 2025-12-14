<?php

declare(strict_types=1);

use Flarme\PhpClickhouse\Exceptions\UnsupportedBindingException;
use Flarme\PhpClickhouse\Query;

describe('Query', function (): void {
    describe('construction', function (): void {
        it('can be instantiated with sql only', function (): void {
            $query = new Query('SELECT * FROM users');

            expect($query->sql)->toBe('SELECT * FROM users')
                ->and($query->bindings)->toBe([]);
        });

        it('can be instantiated with sql and bindings', function (): void {
            $query = new Query('SELECT * FROM users WHERE id = ?', [1]);

            expect($query->sql)->toBe('SELECT * FROM users WHERE id = ?')
                ->and($query->bindings)->toBe([1]);
        });

        it('can be created using static from method', function (): void {
            $query = Query::from('SELECT * FROM users', ['test']);

            expect($query)->toBeInstanceOf(Query::class)
                ->and($query->sql)->toBe('SELECT * FROM users')
                ->and($query->bindings)->toBe(['test']);
        });
    });

    describe('getBindings', function (): void {
        it('returns the bindings array', function (): void {
            $bindings = [1, 'test', 3.14];
            $query = new Query('SELECT * FROM users', $bindings);

            expect($query->getBindings())->toBe($bindings);
        });

        it('returns empty array when no bindings', function (): void {
            $query = new Query('SELECT * FROM users');

            expect($query->getBindings())->toBe([]);
        });
    });

    describe('toSql', function (): void {
        it('converts anonymous placeholders to clickhouse format', function (): void {
            $query = new Query('SELECT * FROM users WHERE id = ? AND name = ?');

            expect($query->toSql())->toBe('SELECT * FROM users WHERE id = {p0:Dynamic} AND name = {p1:Dynamic}');
        });

        it('converts named placeholders with colon to clickhouse format', function (): void {
            $query = new Query('SELECT * FROM users WHERE id = :id AND name = :name');

            expect($query->toSql())->toBe('SELECT * FROM users WHERE id = {id:Dynamic} AND name = {name:Dynamic}');
        });

        it('converts curly brace placeholders without type to clickhouse format', function (): void {
            $query = new Query('SELECT * FROM users WHERE id = {id}');

            expect($query->toSql())->toBe('SELECT * FROM users WHERE id = {id:Dynamic}');
        });

        it('preserves curly brace placeholders with type', function (): void {
            $query = new Query('SELECT * FROM users WHERE id = {id:UInt64}');

            expect($query->toSql())->toBe('SELECT * FROM users WHERE id = {id:UInt64}');
        });

        it('does not process placeholders inside single quoted strings', function (): void {
            $query = new Query("SELECT * FROM users WHERE name = 'test ? value'");

            expect($query->toSql())->toBe("SELECT * FROM users WHERE name = 'test ? value'");
        });

        it('does not process placeholders inside double quoted strings', function (): void {
            $query = new Query('SELECT * FROM users WHERE name = "test ? value"');

            expect($query->toSql())->toBe('SELECT * FROM users WHERE name = "test ? value"');
        });

        it('handles escaped single quotes in strings', function (): void {
            $query = new Query("SELECT * FROM users WHERE name = 'test''s value'");

            expect($query->toSql())->toBe("SELECT * FROM users WHERE name = 'test''s value'");
        });

        it('handles named parameters starting with underscore', function (): void {
            $query = new Query('SELECT * FROM users WHERE id = :_id');

            expect($query->toSql())->toBe('SELECT * FROM users WHERE id = {_id:Dynamic}');
        });

        it('handles named parameters with numbers', function (): void {
            $query = new Query('SELECT * FROM users WHERE id = :id123');

            expect($query->toSql())->toBe('SELECT * FROM users WHERE id = {id123:Dynamic}');
        });

        it('does not treat colon followed by non-alpha as named parameter', function (): void {
            $query = new Query('SELECT * FROM users WHERE time > :123');

            expect($query->toSql())->toBe('SELECT * FROM users WHERE time > :123');
        });

        it('handles unclosed curly brace', function (): void {
            $query = new Query('SELECT * FROM users WHERE id = {id');

            expect($query->toSql())->toBe('SELECT * FROM users WHERE id = {id');
        });

        it('handles mixed placeholder types', function (): void {
            $query = new Query('SELECT * FROM users WHERE id = ? AND name = :name AND status = {status:String}');

            expect($query->toSql())->toBe('SELECT * FROM users WHERE id = {p0:Dynamic} AND name = {name:Dynamic} AND status = {status:String}');
        });
    });

    describe('toRawSql', function (): void {
        it('substitutes string bindings with escaped quotes', function (): void {
            $query = new Query('SELECT * FROM users WHERE name = ?', ['John']);

            expect($query->toRawSql())->toBe("SELECT * FROM users WHERE name = 'John'");
        });

        it('substitutes integer bindings', function (): void {
            $query = new Query('SELECT * FROM users WHERE id = ?', [42]);

            expect($query->toRawSql())->toBe('SELECT * FROM users WHERE id = 42');
        });

        it('substitutes float bindings', function (): void {
            $query = new Query('SELECT * FROM users WHERE price = ?', [3.14]);

            expect($query->toRawSql())->toBe('SELECT * FROM users WHERE price = 3.14');
        });

        it('substitutes boolean bindings', function (): void {
            $query = new Query('SELECT * FROM users WHERE active = ?', [true]);

            expect($query->toRawSql())->toBe('SELECT * FROM users WHERE active = 1');
        });

        it('substitutes null bindings', function (): void {
            $query = new Query('SELECT * FROM users WHERE deleted_at = ?', [null]);

            expect($query->toRawSql())->toBe('SELECT * FROM users WHERE deleted_at = NULL');
        });

        it('substitutes array bindings', function (): void {
            $query = new Query('SELECT * FROM users WHERE id IN ?', [[1, 2, 3]]);

            expect($query->toRawSql())->toBe('SELECT * FROM users WHERE id IN [1, 2, 3]');
        });

        it('substitutes array of strings bindings', function (): void {
            $query = new Query('SELECT * FROM users WHERE name IN ?', [['John', 'Jane']]);

            expect($query->toRawSql())->toBe("SELECT * FROM users WHERE name IN ['John', 'Jane']");
        });

        it('substitutes DateTime bindings', function (): void {
            $date = new DateTime('2024-01-15 10:30:00');
            $query = new Query('SELECT * FROM users WHERE created_at = ?', [$date]);

            expect($query->toRawSql())->toBe('SELECT * FROM users WHERE created_at = ' . $date->getTimestamp());
        });

        it('substitutes Stringable bindings', function (): void {
            $stringable = new class () implements Stringable {
                public function __toString(): string
                {
                    return 'stringable_value';
                }
            };
            $query = new Query('SELECT * FROM users WHERE name = ?', [$stringable]);

            expect($query->toRawSql())->toBe("SELECT * FROM users WHERE name = 'stringable_value'");
        });

        it('escapes single quotes in string bindings', function (): void {
            $query = new Query('SELECT * FROM users WHERE name = ?', ["O'Brien"]);

            expect($query->toRawSql())->toBe("SELECT * FROM users WHERE name = 'O''Brien'");
        });

        it('escapes backslashes in string bindings', function (): void {
            $query = new Query('SELECT * FROM users WHERE path = ?', ['C:\\Users']);

            expect($query->toRawSql())->toBe("SELECT * FROM users WHERE path = 'C:\\\\Users'");
        });

        it('substitutes named bindings', function (): void {
            $query = new Query('SELECT * FROM users WHERE id = :id', ['id' => 42]);

            expect($query->toRawSql())->toBe('SELECT * FROM users WHERE id = 42');
        });

        it('preserves placeholder when binding key not found', function (): void {
            $query = new Query('SELECT * FROM users WHERE id = {id:UInt64}', []);

            expect($query->toRawSql())->toBe('SELECT * FROM users WHERE id = {id:UInt64}');
        });

        it('throws exception for unsupported binding type', function (): void {
            $query = new Query('SELECT * FROM users WHERE data = ?', [fopen('php://memory', 'r')]);

            expect(fn() => $query->toRawSql())->toThrow(UnsupportedBindingException::class);
        });

        it('throws exception for object binding without Stringable', function (): void {
            $query = new Query('SELECT * FROM users WHERE data = ?', [new stdClass()]);

            expect(fn() => $query->toRawSql())->toThrow(UnsupportedBindingException::class);
        });
    });

    describe('toMultipart', function (): void {
        it('returns multipart array with query and bindings', function (): void {
            $query = new Query('SELECT * FROM users WHERE id = ?', [42]);

            $multipart = $query->toMultipart();

            expect($multipart)->toBeArray()
                ->and($multipart)->toHaveCount(2)
                ->and($multipart[0])->toBe(['name' => 'param_p0', 'contents' => 42])
                ->and($multipart[1])->toBe([
                    'name' => 'query', 'contents' => 'SELECT * FROM users WHERE id = {p0:Dynamic}',
                ]);
        });

        it('handles multiple bindings', function (): void {
            $query = new Query('SELECT * FROM users WHERE id = ? AND name = ?', [42, 'John']);

            $multipart = $query->toMultipart();

            expect($multipart)->toHaveCount(3)
                ->and($multipart[0])->toBe(['name' => 'param_p0', 'contents' => 42])
                ->and($multipart[1])->toBe(['name' => 'param_p1', 'contents' => '\'John\'']);
        });

        it('handles named bindings', function (): void {
            $query = new Query('SELECT * FROM users WHERE id = :id', ['id' => 42]);

            $multipart = $query->toMultipart();

            expect($multipart[0])->toBe(['name' => 'param_id', 'contents' => 42]);
        });

        it('encodes DateTime bindings as timestamp', function (): void {
            $date = new DateTime('2024-01-15 10:30:00');
            $query = new Query('SELECT * FROM users WHERE created_at = ?', [$date]);

            $multipart = $query->toMultipart();

            expect($multipart[0]['contents'])->toBe($date->getTimestamp());
        });

        it('encodes boolean bindings', function (): void {
            $query = new Query('SELECT * FROM users WHERE active = ?', [true]);

            $multipart = $query->toMultipart();

            expect($multipart[0]['contents'])->toBe(true);
        });

        it('encodes null bindings as NULL', function (): void {
            $query = new Query('SELECT * FROM users WHERE deleted_at = ?', [null]);

            $multipart = $query->toMultipart();

            expect($multipart[0]['contents'])->toBe('NULL');
        });

        it('encodes array bindings', function (): void {
            $query = new Query('SELECT * FROM users WHERE id IN ?', [[1, 2, 3]]);

            $multipart = $query->toMultipart();

            expect($multipart[0]['contents'])->toBe('[1, 2, 3]');
        });

        it('throws exception for unsupported binding type', function (): void {
            $query = new Query('SELECT * FROM users WHERE data = ?', [fopen('php://memory', 'r')]);

            expect(fn() => $query->toMultipart())->toThrow(UnsupportedBindingException::class);
        });
    });
});
