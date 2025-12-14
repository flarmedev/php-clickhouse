<?php

declare(strict_types=1);

namespace Flarme\PhpClickhouse\Concerns;

use DateTimeInterface;
use Flarme\PhpClickhouse\Contracts\ExpressionInterface;
use Flarme\PhpClickhouse\Exceptions\UnsupportedBindingException;
use Stringable;

trait EncodesSql
{
    /**
     * Encode a value for use in multipart form data or parameter binding.
     *
     * @param  mixed  $value  The value to encode
     * @return mixed The encoded value
     * @throws UnsupportedBindingException When the value type is not supported
     */
    public function encode(mixed $value): mixed
    {
        if ($value instanceof DateTimeInterface) {
            return $value->getTimestamp();
        }

        if ($value instanceof Stringable) {
            return $this->escapeString((string) $value);
        }

        return match (gettype($value)) {
            'string' => $this->escapeString($value),
            'boolean', 'integer', 'double' => $value,
            'NULL' => 'NULL',
            'array' => match (true) {
                array_is_list($value) => '[' . implode(
                    ', ',
                    array_map(fn($item) => $this->encode($item), $value)
                ) . ']',
                default => json_encode($value),
            },
            default => throw UnsupportedBindingException::forType(gettype($value)),
        };
    }

    /**
     * Wrap a column identifier in keyword identifiers.
     *
     * @param  string|ExpressionInterface  $identifier  The value to wrap
     * @return string The wrapped identifier
     */
    public function wrap(string|ExpressionInterface $identifier): string
    {
        if ($identifier instanceof ExpressionInterface) {
            return (string) $identifier;
        }

        if ($identifier === '*') {
            return $identifier;
        }

        $identifier = mb_strtolower(mb_trim($identifier));

        $aliased = [];

        foreach (explode(' as ', $identifier, 2) as $expression) {
            $segments = [];

            foreach (explode('.', $expression, 2) as $segment) {
                $segments[] = $this->escapeIdentifier(mb_trim($segment));
            }

            $aliased[] = implode('.', $segments);
        }

        return implode(' AS ', $aliased);
    }

    /**
     * Escape a string value for SQL.
     *
     * @param  string  $value  The string to escape
     * @return string The escaped string with surrounding quotes
     */
    protected function escapeString(string $value): string
    {
        return "'" . strtr($value, ['\'' => '\'\'', '\\' => '\\\\']) . "'";
    }

    /**
     * Escape an identifier for SQL.
     *
     * @param  string  $identifier  The identifier to escape
     * @return string The escaped identifier with surrounding quotes
     */
    protected function escapeIdentifier(string $identifier): string
    {
        return '`' . strtr($identifier, ['`' => '``', '\\' => '\\\\']) . '`';
    }
}
