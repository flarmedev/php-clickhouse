<?php

declare(strict_types=1);

use Flarme\PhpClickhouse\Client;
use Flarme\PhpClickhouse\Exceptions\ClickhouseException;

describe('Client', function (): void {
    it('can be instantiated with various configurations', function (array $params, ?string $expectedDb): void {
        $client = new Client(...$params);

        expect($client)->toBeInstanceOf(Client::class)
            ->and($client->database)->toBe($expectedDb);
    })->with([
        'minimal' => [['localhost', 8123, 'default', ''], null],
        'with database' => [['localhost', 8123, 'default', '', 'test_db'], 'test_db'],
        'with secure' => [['localhost', 8443, 'default', '', null, true], null],
        'with options' => [['localhost', 8123, 'default', '', null, false, ['timeout' => 30]], null],
        'with settings' => [['localhost', 8123, 'default', '', null, false, [], ['max_execution_time' => 60]], null],
    ]);

    it('can change database and returns self for fluent interface', function (): void {
        $client = new Client('localhost', 8123, 'default', '');

        $result = $client->database('new_db');

        expect($result)->toBe($client)
            ->and($client->database)->toBe('new_db');
    });

    it('throws exception when inserting empty rows', function (): void {
        $client = new Client('localhost', 8123, 'default', '');

        expect(fn() => $client->insert('users', []))
            ->toThrow(ClickhouseException::class, 'Cannot insert empty rows');
    });

    it('throws exception when inserting from invalid file', function (): void {
        $client = new Client('localhost', 8123, 'default', '');
        $file = false;

        expect(fn() => $client->insertFromFile('users', $file))
            ->toThrow(ClickhouseException::class, 'File not found');
    });
});
