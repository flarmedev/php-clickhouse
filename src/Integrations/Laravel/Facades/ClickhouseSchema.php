<?php

declare(strict_types=1);

namespace Flarme\PhpClickhouse\Integrations\Laravel\Facades;

use Flarme\PhpClickhouse\Schema\Builder;
use Illuminate\Support\Facades\Facade;

/**
 * @method static Builder schema()
 */
class ClickhouseSchema extends Facade
{
    protected static function getFacadeAccessor(): string
    {
        return 'clickhouse.schema';
    }
}
