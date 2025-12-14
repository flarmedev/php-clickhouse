<?php

declare(strict_types=1);

namespace Flarme\PhpClickhouse\Exceptions;

use Exception;

/**
 * Exception thrown when an unsupported binding type is encountered.
 */
class UnsupportedBindingException extends Exception
{
    /**
     * Create an exception for an unsupported type.
     *
     * @param string $type The unsupported type name
     * @return self
     */
    public static function forType(string $type): self
    {
        return new self("Unsupported binding type: {$type}. Supported types are: boolean, integer, double, string, NULL, array, DateTimeInterface, and Stringable objects.");
    }

    /**
     * Create an exception for an unsupported value.
     *
     * @param mixed $value The unsupported value
     * @return self
     */
    public static function forValue(mixed $value): self
    {
        $type = is_object($value) ? get_class($value) : gettype($value);

        return self::forType($type);
    }
}
