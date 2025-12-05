<?php

declare(strict_types=1);

namespace Flarme\PhpClickhouse\Contracts;

interface ClientInterface
{
    public function execute(string|QueryInterface $query, array $bindings): ResponseInterface;
}
