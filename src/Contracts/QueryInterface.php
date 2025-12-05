<?php

declare(strict_types=1);

namespace Flarme\PhpClickhouse\Contracts;

interface QueryInterface
{
    public function toSql(): string;

    public function toMultipart(): array;
}
