<?php

declare(strict_types=1);

namespace Flarme\PhpClickhouse\Contracts;

interface ExpressionInterface
{
    public function __toString(): string;
}
