<?php

declare(strict_types=1);

namespace Flarme\PhpClickhouse\Expressions;

use Flarme\PhpClickhouse\Contracts\ExpressionInterface;

final class Raw implements ExpressionInterface
{
    public private(set) string $expression;

    public function __construct(string $expression)
    {
        $this->expression = $expression;
    }

    public function __toString(): string
    {
        return $this->expression;
    }

    /**
     * Create a new raw expression.
     *
     * @param  string  $expression  The raw SQL expression
     * @return self
     */
    public static function make(string $expression): self
    {
        return new self($expression);
    }
}
