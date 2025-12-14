<?php

declare(strict_types=1);

use Flarme\PhpClickhouse\Concerns\EncodesSql;
use Flarme\PhpClickhouse\Exceptions\UnsupportedBindingException;

describe('ProcessSqlValues Trait', function (): void {
    // Create a dummy class to use the trait for testing
    $processor = new class () {
        use EncodesSql;
    };

    describe('encode', function (): void {
        it('encodes DateTimeInterface to timestamp', function ($processor): void {
            $date = new DateTime('2024-01-15 10:30:00');
            expect($processor->encode($date))->toBe($date->getTimestamp());
        });

        it('encodes Stringable objects', function ($processor): void {
            $stringable = new class () implements Stringable {
                public function __toString(): string
                {
                    return "I'm a string";
                }
            };
            expect($processor->encode($stringable))->toBe("'I''m a string'");
        });

        it('encodes and escapes strings', function ($processor): void {
            expect($processor->encode("hello"))->toBe("'hello'")
                ->and($processor->encode("O'Malley"))->toBe("'O''Malley'")
                ->and($processor->encode("C:\\path"))->toBe("'C:\\\\path'");
        });

        it('encodes booleans', function ($processor): void {
            expect($processor->encode(true))->toBe(true)
                ->and($processor->encode(false))->toBe(false);
        });

        it('encodes integers and floats', function ($processor): void {
            expect($processor->encode(123))->toBe(123)
                ->and($processor->encode(3.14))->toBe(3.14);
        });

        it('encodes null to NULL string', function ($processor): void {
            expect($processor->encode(null))->toBe('NULL');
        });

        it('encodes list arrays', function ($processor): void {
            expect($processor->encode([1, 2, 3]))->toBe('[1, 2, 3]')
                ->and($processor->encode(['a', 'b', 'c']))->toBe("['a', 'b', 'c']");
        });

        it('encodes nested list arrays', function ($processor): void {
            $encoded = $processor->encode([1, [2, 'nested']]);
            expect($encoded)->toBe("[1, [2, 'nested']]");
        });

        it('encodes associative arrays to JSON', function ($processor): void {
            $data = ['a' => 1, 'b' => 'test'];
            expect($processor->encode($data))->toBe(json_encode($data));
        });

        it('throws exception for unsupported types', function ($processor): void {
            expect(fn() => $processor->encode(new stdClass()))
                ->toThrow(UnsupportedBindingException::class);

            $resource = fopen('php://memory', 'r');
            expect(fn() => $processor->encode($resource))
                ->toThrow(UnsupportedBindingException::class);
        });
    });

    describe('wrap', function (): void {
        it('wraps a simple identifier', function ($processor): void {
            expect($processor->wrap('users'))->toBe('`users`');
        });

        it('wraps a multipart identifier', function ($processor): void {
            expect($processor->wrap('database.users'))->toBe('`database`.`users`');
        });

        it('does not wrap the wildcard identifier', function ($processor): void {
            expect($processor->wrap('*'))->toBe('*');
        });

        it('wraps an identifier with an alias', function ($processor): void {
            expect($processor->wrap('users as u'))->toBe('`users` AS `u`');
        });

        it('wraps a multipart identifier with an alias', function ($processor): void {
            expect($processor->wrap('database.users as u'))->toBe('`database`.`users` AS `u`');
        });

        it('trims whitespace and converts to lowercase', function ($processor): void {
            expect($processor->wrap('  USERS  '))->toBe('`users`')
                ->and($processor->wrap('  DATABASE.USERS   AS   U  '))
                ->toBe('`database`.`users` AS `u`');
        });

        it('escapes backticks in identifiers', function ($processor): void {
            expect($processor->wrap('user`s'))->toBe('`user``s`');
        });
    });
})->with([
    'processor' => new class () {
        use EncodesSql;
    },
]);
