<?php

declare(strict_types=1);

namespace Flarme\PhpClickhouse\Extra\Laravel\Facades;

use Flarme\PhpClickhouse\Query\Builder;
use Illuminate\Support\Facades\Facade;

/**
 * @method static Builder query()
 */
class ClickhouseQuery extends Facade
{
    protected static function getFacadeAccessor(): string
    {
        return 'clickhouse.query';
    }
}
