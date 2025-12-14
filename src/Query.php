<?php

declare(strict_types=1);

namespace Flarme\PhpClickhouse;

use Flarme\PhpClickhouse\Concerns\EncodesSql;
use Flarme\PhpClickhouse\Contracts\QueryInterface;
use Flarme\PhpClickhouse\Exceptions\UnsupportedBindingException;

class Query implements QueryInterface
{
    use EncodesSql;

    public string $sql;

    public array $bindings;

    public function __construct(string $sql, array $bindings = [])
    {
        $this->sql = $sql;
        $this->bindings = $bindings;
    }

    public static function from(string $sql, array $bindings = []): self
    {
        return new self($sql, $bindings);
    }

    public function getBindings(): array
    {
        return $this->bindings;
    }

    /**
     * @throws UnsupportedBindingException
     */
    public function toRawSql(): string
    {
        return $this->substituteBindings($this->toSql(), $this->bindings);
    }

    public function toSql(): string
    {
        return $this->resolveBindings($this->sql);
    }

    /**
     * @throws UnsupportedBindingException
     */
    public function toMultipart(): array
    {
        $multipart = [];

        foreach ($this->bindings as $key => $binding) {
            $multipart[] = [
                'name' => 'param_' . (is_int($key) ? "p{$key}" : $key), 'contents' => $this->encode($binding),
            ];
        }

        $multipart[] = ['name' => 'query', 'contents' => $this->toSql()];

        return $multipart;
    }

    private function resolveBindings(string $query): string
    {
        $length = mb_strlen($query);
        $output = '';
        $index = 0;
        $anonymousIndex = 0;
        $inSingleQuote = false;
        $inDoubleQuote = false;

        while ($index < $length) {
            $char = $query[$index];

            // Track string literals to avoid processing placeholders inside them
            if ($char === "'" && ! $inDoubleQuote) {
                // Check for escaped quote
                if ($index + 1 < $length && $query[$index + 1] === "'") {
                    $output .= "''";
                    $index += 2;
                    continue;
                }
                $inSingleQuote = ! $inSingleQuote;
                $output .= $char;
                $index++;
                continue;
            }

            if ($char === '"' && ! $inSingleQuote) {
                $inDoubleQuote = ! $inDoubleQuote;
                $output .= $char;
                $index++;
                continue;
            }

            // Skip placeholder processing inside string literals
            if ($inSingleQuote || $inDoubleQuote) {
                $output .= $char;
                $index++;
                continue;
            }

            switch ($char) {
                case '?':
                    $output .= '{p' . $anonymousIndex++ . ':Dynamic}';
                    $index++;

                    continue 2;
                case '{':
                    $end = mb_strpos($query, '}', $index);
                    if ($end === false) {
                        $output .= $char;
                        $index++;
                        continue 2;
                    }
                    $colon = mb_strpos($query, ':', $index);

                    if ($colon !== false && $colon < $end) {
                        $output .= mb_substr($query, $index, $end - $index + 1);
                    } else {
                        $output .= mb_substr($query, $index, $end - $index) . ':Dynamic}';
                    }

                    $index = $end + 1;

                    continue 2;
                case ':':
                    $subIndex = $index + 1;

                    // Named parameter must start with a letter or underscore
                    if ($subIndex < $length && (ctype_alpha($query[$subIndex]) || $query[$subIndex] === '_')) {
                        while ($subIndex < $length && ($query[$subIndex] === '_' || ctype_alnum($query[$subIndex]))) {
                            $subIndex++;
                        }

                        $paramName = mb_substr($query, $index + 1, $subIndex - $index - 1);
                        $output .= '{' . $paramName . ':Dynamic}';
                        $index = $subIndex;

                        continue 2;
                    }

                    // Not a named parameter, just output the colon
                    $output .= $char;
                    $index++;

                    continue 2;
            }

            $output .= $char;
            $index++;
        }

        return $output;
    }

    /**
     * @throws UnsupportedBindingException
     */
    private function substituteBindings(string $query, array $bindings): string
    {
        $length = mb_strlen($query);
        $output = '';
        $index = 0;

        while ($index < $length) {
            $start = mb_strpos($query, '{', $index);

            if ($start === false) {
                $output .= mb_substr($query, $index);
                break;
            }

            $output .= mb_substr($query, $index, $start - $index);

            $colonPos = mb_strpos($query, ':', $start);
            $end = mb_strpos($query, '}', $start);

            if ($colonPos === false || $end === false || $colonPos > $end) {
                // Not a valid placeholder, just output the character and continue
                $output .= '{';
                $index = $start + 1;
                continue;
            }

            $key = mb_substr($query, $start + 1, $colonPos - $start - 1);

            if (
                str_starts_with($key, 'p')
                && is_int($index = (int) mb_substr($key, 1))
            ) {
                $key = $index;
            }

            if (array_key_exists($key, $bindings)) {
                $output .= $this->encode($bindings[$key]);
            } else {
                // Key not found in bindings, output placeholder as-is
                $output .= mb_substr($query, $start, $end - $start + 1);
            }

            $index = $end + 1;
        }

        return $output;
    }
}
