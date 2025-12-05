<?php

declare(strict_types=1);

namespace Flarme\PhpClickhouse;

use DateTimeInterface;
use Flarme\PhpClickhouse\Contracts\QueryInterface;
use Flarme\PhpClickhouse\Exceptions\UnsupportedBindingException;
use Stringable;

class Query implements QueryInterface
{
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
            $multipart[] = ['name' => 'param_' . $key, 'contents' => $this->encode($binding)];
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

        while ($index < $length) {
            $char = $query[$index];

            switch ($char) {
                case '?':
                    $output .= '{' . $anonymousIndex++ . ':Dynamic}';
                    $index++;

                    continue 2;
                case '{':
                    $end = mb_strpos($query, '}', $index);
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

                    while ($subIndex < $length && ($query[$subIndex] === '_' || ctype_alnum($query[$subIndex]))) {
                        $subIndex++;
                    }

                    $subIndex--;

                    if ($subIndex === $index + 1) {
                        break;
                    }

                    $output .= '{' . mb_substr($query, $index + 1, $subIndex - $index) . ':Dynamic}';
                    $index = $subIndex + 1;

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

            if ( ! $start) {
                $output .= mb_substr($query, $index);
                $index = $length;

                continue;
            }

            $output .= mb_substr($query, $index, $start - $index);

            $key = mb_substr($query, $start + 1, mb_strpos($query, ':', $start) - $start - 1);

            $output .= $this->encodeRaw($bindings[$key]);

            $end = mb_strpos($query, '}', $start);
            $index = $end + 1;
        }

        return $output;
    }

    /**
     * @throws UnsupportedBindingException
     */
    private function encode(mixed $value): mixed
    {
        if ($value instanceof DateTimeInterface) {
            return $value->getTimestamp();
        }

        if ($value instanceof Stringable) {
            return (string) $value;
        }

        return match (gettype($value)) {
            'boolean', 'integer', 'double', 'string' => $value,
            'NULL' => '\N',
            'array' => '[' . implode(', ', array_map(fn($item) => $this->encodeRaw($item), $value)) . ']',
            default => throw new UnsupportedBindingException(),
        };
    }

    /**
     * @throws UnsupportedBindingException
     */
    private function encodeRaw(mixed $value): mixed
    {
        if (is_string($value) || $value instanceof Stringable) {
            return "'" . strtr((string) $value, ['\'' => '\'\'', '\\' => '\\\\']) . "'";
        }

        return $this->encode($value);
    }
}
