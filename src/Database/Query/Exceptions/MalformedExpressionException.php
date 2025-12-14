<?php

declare(strict_types=1);

namespace Flarme\PhpClickhouse\Database\Query\Exceptions;

use Exception;

class MalformedExpressionException extends Exception
{
    public function __construct(string $message = '', int $code = 0, ?Exception $previous = null)
    {
        parent::__construct('Malformed expression:' . PHP_EOL . $message, $code, $previous);
    }
}
