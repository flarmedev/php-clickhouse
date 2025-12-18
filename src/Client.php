<?php

declare(strict_types=1);

namespace Flarme\PhpClickhouse;

use Exception;
use Flarme\PhpClickhouse\Concerns\EncodesSql;
use Flarme\PhpClickhouse\Contracts\ClientInterface;
use Flarme\PhpClickhouse\Contracts\QueryInterface;
use Flarme\PhpClickhouse\Contracts\ResponseInterface;
use Flarme\PhpClickhouse\Database\Query\Builder as QueryBuilder;
use Flarme\PhpClickhouse\Database\Schema\Builder as SchemaBuilder;
use Flarme\PhpClickhouse\Exceptions\ClickhouseException;
use Flarme\PhpClickhouse\Exceptions\UnsupportedBindingException;
use GuzzleHttp\Client as HttpClient;
use GuzzleHttp\Exception\GuzzleException;

class Client implements ClientInterface
{
    use EncodesSql;

    public ?string $database {
        get {
            return $this->database;
        }
    }

    private HttpClient $httpClient;

    /**
     * @param  array  $options  Guzzle options
     * @param  array  $settings  Clickhouse settings
     *
     * @see https://docs.guzzlephp.org/en/stable/request-options.html
     * @see https://clickhouse.com/docs/operations/settings/settings
     */
    public function __construct(
        string $host,
        int $port,
        string $username,
        string $password,
        ?string $database = null,
        bool $secure = false,
        array $options = [],
        array $settings = [],
    ) {
        $protocol = $secure ? 'https' : 'http';

        $configuration = [
            'base_uri' => "{$protocol}://{$host}:{$port}",
            'auth' => [$username, $password],
            'query' => $settings,
            'connect_timeout' => 5,
            ...$options,
        ];

        $this->httpClient = new HttpClient($configuration);
        $this->database = $database;
    }

    /**
     * @return $this
     */
    public function database(string $database): self
    {
        $this->database = $database;

        return $this;
    }

    public function query(): QueryBuilder
    {
        return new QueryBuilder($this);
    }

    public function schema(): SchemaBuilder
    {
        return new SchemaBuilder($this);
    }

    /**
     * @throws ClickhouseException
     * @throws UnsupportedBindingException
     */
    public function execute(string|QueryInterface $query, array $bindings = []): ResponseInterface
    {
        if (is_string($query)) {
            $query = Query::from($query, $bindings);
        }

        try {
            $response = $this->httpClient->post('/', [
                'headers' => [
                    'x-clickhouse-database' => $this->database,
                    'x-clickhouse-format' => 'NDJSON',
                ],
                'multipart' => $query->toMultipart(),
            ]);
        } catch (Exception $e) {
            throw new ClickhouseException($e->getMessage(), $e->getCode(), $e);
        }

        return new Response($response);
    }

    /**
     * Insert rows into a table using the HTTP body (avoids multipart form field size limits).
     *
     * @param  string  $table  Table name
     * @param  array<int, array<string, mixed>>  $rows  Array of rows to insert
     * @param  array<string>|null  $columns  Column names (optional, will be inferred from first row if not provided)
     * @return ResponseInterface
     *
     * @throws ClickhouseException
     */
    public function insert(string $table, array $rows, ?array $columns = null): ResponseInterface
    {
        if (empty($rows)) {
            throw new ClickhouseException('Cannot insert empty rows');
        }

        if ($columns === null) {
            $columns = array_keys($rows[0]);
        }

        $keys = implode(', ', array_map(fn($col) => $this->wrap($col), $columns));

        try {
            $response = $this->httpClient->post('/', [
                'headers' => [
                    'x-clickhouse-database' => $this->database,
                    'x-clickhouse-format' => 'NDJSON',
                ],
                'query' => ['query' => "INSERT INTO {$this->wrap($table)} ({$keys}) VALUES"],
                'body' => mb_rtrim(implode(
                    ', ',
                    array_map(
                        fn($row) => '(' . implode(', ', array_map([$this, 'encode'], $row)) . ')',
                        $rows
                    )
                )),
            ]);
        } catch (GuzzleException $e) {
            throw new ClickhouseException($e->getMessage(), $e->getCode(), $e);
        }

        return new Response($response);
    }

    /**
     * Insert data from a file (CSV, TSV, JSONEachRow, etc.).
     *
     * @param  string  $table  Table name
     * @param  resource  $file  File
     * @param  string  $format  ClickHouse format (CSV, TSV, JSONEachRow, etc.)
     * @param  array<string>|null  $columns  Column names (optional)
     * @return ResponseInterface
     *
     * @throws ClickhouseException
     */
    public function insertFromFile(
        string $table,
        &$file,
        string $format = 'JSONEachRow',
        ?array $columns = null
    ): ResponseInterface {
        if ( ! is_resource($file)) {
            throw new ClickhouseException("File not found");
        }

        rewind($file);

        $query = "INSERT INTO {$this->wrap($table)}";

        if ( ! empty($columns)) {
            $query .= ' (' . implode(', ', array_map(fn($col) => "`{$col}`", $columns)) . ')';
        }

        $query .= ' VALUES';

        try {
            $response = $this->httpClient->post('/', [
                'headers' => [
                    'x-clickhouse-database' => $this->database,
                    'x-clickhouse-format' => $format,
                ],
                'query' => ['query' => $query],
                'body' => $file,
            ]);
        } catch (Exception $e) {
            throw new ClickhouseException($e->getMessage(), $e->getCode(), $e);
        } finally {
            fclose($file);
        }

        return new Response($response);
    }
}
