<?php

declare(strict_types=1);

use Flarme\PhpClickhouse\Query;

it('can compile a query', function (): void {
    $query = new Query('select 1');

    expect($query->toSql())->toBe('select 1');
});

it('can resolve bindings', function (): void {
    $query = new Query('select ?, :param, {param}, {param:String}');

    expect($query->toSql())->toBe('select {0:Dynamic}, {param:Dynamic}, {param:Dynamic}, {param:String}');
});

it('can substitute bindings', function (): void {
    $query = new Query('select ?, {param:String}', ['anonymous', 'param' => 'named']);

    expect($query->toRawSql())->toBe('select \'anonymous\', \'named\'');
});

it('can compile a multipart query and encode bindings', function (): void {
    $datetime = new DateTime();
    $stringable = new class() implements Stringable
    {
        public function __toString()
        {
            return 'stringable';
        }
    };

    $query = new Query('select 1', [
        'string',
        $datetime,
        $stringable,
        true,
        23.43,
        ['foo', 4],
        null,
    ]);

    expect($query->toMultipart())->toMatchArray([
        ['name' => 'param_0', 'contents' => 'string'],
        ['name' => 'param_1', 'contents' => $datetime->getTimestamp()],
        ['name' => 'param_2', 'contents' => 'stringable'],
        ['name' => 'param_3', 'contents' => true],
        ['name' => 'param_4', 'contents' => 23.43],
        ['name' => 'param_5', 'contents' => '[\'foo\', 4]'],
        ['name' => 'param_6', 'contents' => '\N'],
    ]);
});
