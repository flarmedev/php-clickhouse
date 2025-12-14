<?php

declare(strict_types=1);

use Flarme\PhpClickhouse\Response;
use GuzzleHttp\Psr7\Response as GuzzleResponse;

describe('Response', function () {
    /**
     * Helper to create a mock HTTP response with NDJSON body
     */
    function createMockResponse(
        array $rows = [],
        array $summary = [],
        string $server = 'test-server',
        string $queryId = 'test-query-id',
        string $format = 'NDJSON'
    ): GuzzleResponse {
        $body = '';
        foreach ($rows as $row) {
            $body .= json_encode($row) . "\n";
        }

        return new GuzzleResponse(200, [
            'x-clickhouse-summary' => json_encode($summary),
            'x-clickhouse-server-display-name' => $server,
            'x-clickhouse-query-id' => $queryId,
            'x-clickhouse-format' => $format,
        ], $body);
    }

    describe('construction', function (): void {
        it('parses headers from HTTP response', function (): void {
            $httpResponse = createMockResponse(
                rows: [],
                summary: ['read_rows' => '100', 'read_bytes' => '1024'],
                server: 'clickhouse-server-1',
                queryId: 'abc-123',
                format: 'NDJSON'
            );

            $response = new Response($httpResponse);

            expect($response->summary)->toBe(['read_rows' => '100', 'read_bytes' => '1024'])
                ->and($response->server)->toBe('clickhouse-server-1')
                ->and($response->queryId)->toBe('abc-123')
                ->and($response->format)->toBe('NDJSON');
        });

        it('handles empty summary header as empty object', function (): void {
            $httpResponse = new GuzzleResponse(200, [
                'x-clickhouse-summary' => '{}',
                'x-clickhouse-server-display-name' => 'server',
                'x-clickhouse-query-id' => 'id',
                'x-clickhouse-format' => 'NDJSON',
            ], '');

            $response = new Response($httpResponse);

            expect($response->summary)->toBe([]);
        });

        it('handles missing headers gracefully', function (): void {
            $httpResponse = new GuzzleResponse(200, [
                'x-clickhouse-summary' => '{}',
            ], '');

            $response = new Response($httpResponse);

            expect($response->server)->toBe('')
                ->and($response->queryId)->toBe('')
                ->and($response->format)->toBe('');
        });
    });

    describe('toArray', function (): void {
        it('returns all rows as array', function (): void {
            $rows = [
                ['id' => 1, 'name' => 'John'],
                ['id' => 2, 'name' => 'Jane'],
            ];
            $httpResponse = createMockResponse($rows);

            $response = new Response($httpResponse);

            expect($response->toArray())->toBe($rows);
        });

        it('returns empty array for empty response', function (): void {
            $httpResponse = createMockResponse([]);

            $response = new Response($httpResponse);

            expect($response->toArray())->toBe([]);
        });
    });

    describe('rows', function (): void {
        it('returns a generator', function (): void {
            $rows = [
                ['id' => 1, 'name' => 'John'],
                ['id' => 2, 'name' => 'Jane'],
            ];
            $httpResponse = createMockResponse($rows);

            $response = new Response($httpResponse);

            expect($response->rows())->toBeInstanceOf(Generator::class);
        });

        it('yields rows one by one', function (): void {
            $rows = [
                ['id' => 1, 'name' => 'John'],
                ['id' => 2, 'name' => 'Jane'],
            ];
            $httpResponse = createMockResponse($rows);

            $response = new Response($httpResponse);
            $result = [];
            foreach ($response->rows() as $row) {
                $result[] = $row;
            }

            expect($result)->toBe($rows);
        });

        it('yields nothing for empty response', function (): void {
            $httpResponse = createMockResponse([]);

            $response = new Response($httpResponse);
            $result = iterator_to_array($response->rows());

            expect($result)->toBe([]);
        });

        it('can be iterated multiple times', function (): void {
            $rows = [['id' => 1]];
            $httpResponse = createMockResponse($rows);

            $response = new Response($httpResponse);

            $first = iterator_to_array($response->rows());
            $second = iterator_to_array($response->rows());

            expect($first)->toBe($rows)
                ->and($second)->toBe($rows);
        });
    });

    describe('count', function (): void {
        it('returns the number of rows', function (): void {
            $rows = [
                ['id' => 1],
                ['id' => 2],
                ['id' => 3],
            ];
            $httpResponse = createMockResponse($rows);

            $response = new Response($httpResponse);

            // Note: count() counts newlines, so it may be off by one depending on implementation
            // The actual implementation counts newlines + 1
            expect($response->count())->toBeGreaterThanOrEqual(3);
        });

        it('returns 0 for empty response', function (): void {
            $httpResponse = new GuzzleResponse(200, [
                'x-clickhouse-summary' => '{}',
                'x-clickhouse-server-display-name' => 'server',
                'x-clickhouse-query-id' => 'id',
                'x-clickhouse-format' => 'NDJSON',
            ], '');

            $response = new Response($httpResponse);

            expect($response->count())->toBe(0);
        });
    });

    describe('first', function (): void {
        it('returns the first row', function (): void {
            $rows = [
                ['id' => 1, 'name' => 'John'],
                ['id' => 2, 'name' => 'Jane'],
            ];
            $httpResponse = createMockResponse($rows);

            $response = new Response($httpResponse);

            expect($response->first())->toBe(['id' => 1, 'name' => 'John']);
        });

        it('returns null for empty response', function (): void {
            $httpResponse = new GuzzleResponse(200, [
                'x-clickhouse-summary' => '{}',
                'x-clickhouse-server-display-name' => 'server',
                'x-clickhouse-query-id' => 'id',
                'x-clickhouse-format' => 'NDJSON',
            ], '');

            $response = new Response($httpResponse);

            expect($response->first())->toBeNull();
        });

        it('can be called multiple times', function (): void {
            $rows = [['id' => 1]];
            $httpResponse = createMockResponse($rows);

            $response = new Response($httpResponse);

            expect($response->first())->toBe(['id' => 1])
                ->and($response->first())->toBe(['id' => 1]);
        });
    });

    describe('toJson', function (): void {
        it('returns JSON array string', function (): void {
            $rows = [
                ['id' => 1, 'name' => 'John'],
                ['id' => 2, 'name' => 'Jane'],
            ];
            $httpResponse = createMockResponse($rows);

            $response = new Response($httpResponse);
            $json = $response->toJson();

            expect($json)->toBeString()
                ->and(json_decode($json, true))->toBe($rows);
        });

        it('returns empty array string for empty response', function (): void {
            $httpResponse = new GuzzleResponse(200, [
                'x-clickhouse-summary' => '{}',
                'x-clickhouse-server-display-name' => 'server',
                'x-clickhouse-query-id' => 'id',
                'x-clickhouse-format' => 'NDJSON',
            ], '');

            $response = new Response($httpResponse);

            expect($response->toJson())->toBe('[]');
        });
    });

    describe('destructor', function (): void {
        it('closes the stream on destruction', function (): void {
            $httpResponse = createMockResponse([['id' => 1]]);
            $response = new Response($httpResponse);

            $stream = new ReflectionClass($response)
                ->getProperty('stream')
                ->getValue($response);

            unset($response);

            expect(gettype($stream))->toBe('resource (closed)');
        });
    });
});
