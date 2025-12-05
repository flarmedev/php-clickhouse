<?php

declare(strict_types=1);

namespace Flarme\PhpClickhouse\Contracts;

use Generator;
use Psr\Http\Message\ResponseInterface as HttpResponseInterface;

interface ResponseInterface
{
    public function __construct(HttpResponseInterface $response);

    public function __destruct();

    public function toArray(): array;

    public function toJson(): string;

    public function rows(): Generator;

    public function count(): int;

    public function first(): mixed;

}
