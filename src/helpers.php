<?php

declare(strict_types=1);

use Flarme\PhpClickhouse\Expressions\Raw;

if ( ! function_exists('raw')) {
    /**
     * Create a new raw query expression.
     */
    function raw(string $value): Raw
    {
        return new Raw($value);
    }
}
