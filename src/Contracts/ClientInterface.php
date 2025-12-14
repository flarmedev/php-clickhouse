<?php

declare(strict_types=1);

namespace Flarme\PhpClickhouse\Contracts;

interface ClientInterface
{
    public function execute(string|QueryInterface $query, array $bindings = []): ResponseInterface;

    /**
     * Insert rows into a table.
     *
     * @param  string  $table  Table name
     * @param  array<int, array<string, mixed>>  $rows  Array of rows to insert
     * @param  array<string>|null  $columns  Column names (optional)
     */
    public function insert(string $table, array $rows, ?array $columns = null): ResponseInterface;

    /**
     * Insert data from a file.
     *
     * @param  string  $table  Table name
     * @param  string  $filePath  Path to the file
     * @param  string  $format  ClickHouse format (CSV, TSV, JSONEachRow, etc.)
     * @param  array<string>|null  $columns  Column names (optional)
     */
    public function insertFromFile(
        string $table,
        &$file,
        string $format = 'JSONEachRow',
        ?array $columns = null
    ): ResponseInterface;
}
