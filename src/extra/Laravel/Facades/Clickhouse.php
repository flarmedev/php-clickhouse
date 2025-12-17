<?php

declare(strict_types=1);

namespace Flarme\PhpClickhouse\Extra\Laravel\Facades;

use Flarme\PhpClickhouse\Client;
use Flarme\PhpClickhouse\Contracts\ResponseInterface;
use Flarme\PhpClickhouse\Database\Query\Builder as QueryBuilder;
use Flarme\PhpClickhouse\Database\Schema\Builder as SchemaBuilder;
use Illuminate\Support\Facades\Facade;

/**
 * @method static Client connection(?string $name = null)
 * @method static Client database(string $database)
 * @method static QueryBuilder query()
 * @method static SchemaBuilder schema()
 * @method static ResponseInterface execute(string $query, array $bindings = [])
 * @method static ResponseInterface insert(string $table, array $rows, ?array $columns = null)
 * @method static ResponseInterface insertFromFile(string $table, resource $file, string $format = 'JSONEachRow', ?array $columns = null)
 * @method static string getDefaultConnection()
 * @method static void setDefaultConnection(string $name)
 *
 * @see \Flarme\PhpClickhouse\Extra\Laravel\ClickhouseManager
 * @see Client
 */
class Clickhouse extends Facade
{
    /**
     * Get the registered name of the component.
     */
    protected static function getFacadeAccessor(): string
    {
        return 'clickhouse.connection';
    }
}
