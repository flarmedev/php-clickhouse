<?php

declare(strict_types=1);

use Flarme\PhpClickhouse\Response;
use GuzzleHttp\Psr7\Response as HttpResponse;

function mockResponse(
    string $body = <<<'NDJSON'
    {"row": "value"}
    {"row": "value2"}
NDJSON
): Response {
    $headers = [
        'x-clickhouse-summary' => '{"read_rows":"2","read_bytes":"8","written_rows":"0","written_bytes":"0","total_rows_to_read":"2"}',
        'x-clickhouse-server-display-name' => 'clickhouse-server',
        'x-clickhouse-query-id' => 'some-query-id',
        'x-clickhouse-format' => 'NDJSON',
    ];
    $httpResponse = new HttpResponse(200, $headers, $body);

    return new Response($httpResponse);
}

it('can construct from guzzle responses', function (): void {
    $response = mockResponse();

    expect($response->summary)->toEqual([
        'read_rows' => '2',
        'read_bytes' => '8',
        'written_rows' => '0',
        'written_bytes' => '0',
        'total_rows_to_read' => '2',
    ])
        ->and($response->server)->toBe('clickhouse-server')
        ->and($response->queryId)->toBe('some-query-id')
        ->and($response->format)->toBe('NDJSON');
});

it('can convert to json', function (): void {
    $response = mockResponse();

    expect($response->toJson())->toBe('[{"row": "value"},{"row": "value2"}]');
});

it('can convert to array', function (): void {
    $response = mockResponse();

    expect($response->toArray())->toBe([['row' => 'value'], ['row' => 'value2']]);
});

it('can convert to generator', function (): void {
    $response = mockResponse();

    $rows = [];

    foreach ($response->rows() as $row) {
        $rows[] = $row;
    }
    expect($rows)->toBe([['row' => 'value'], ['row' => 'value2']]);
});

it('can count rows', function (): void {
    $response = mockResponse();

    expect($response->count())->toBe(2);
});

it('can get the first result', function (): void {
    $response = mockResponse();

    expect($response->first())->toBe(['row' => 'value']);
});
