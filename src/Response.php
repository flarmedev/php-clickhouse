<?php

declare(strict_types=1);

namespace Flarme\PhpClickhouse;

use Flarme\PhpClickhouse\Contracts\ResponseInterface;
use Generator;
use Psr\Http\Message\ResponseInterface as HttpResponseInterface;

class Response implements ResponseInterface
{
    public array $summary;

    public string $server;

    public string $queryId;

    public string $format;

    /** @var null|resource */
    private $stream;

    public function __construct(HttpResponseInterface $response)
    {
        $this->summary = json_decode($response->getHeaderLine('x-clickhouse-summary'), true) ?? [];
        $this->server = $response->getHeaderLine('x-clickhouse-server-display-name');
        $this->queryId = $response->getHeaderLine('x-clickhouse-query-id');
        $this->format = $response->getHeaderLine('x-clickhouse-format');
        $this->stream = $response->getBody()->detach();
    }

    public function __destruct()
    {
        if (isset($this->stream)) {
            fclose($this->stream);
        }
    }

    /**
     * /!\ Use with caution, this method may cause unexcepted behavior due to memory consumption on large datasets.
     */
    public function toArray(): array
    {
        $buffer = [];

        if ( ! isset($this->stream)) {
            return $buffer;
        }

        rewind($this->stream);

        while ( ! feof($this->stream)) {
            $line = fgets($this->stream);

            if ( ! $line) {
                continue;
            }

            $buffer[] = json_decode($line, true);
        }

        return $buffer;
    }

    public function rows(): Generator
    {
        if ( ! isset($this->stream)) {
            yield from [];
        } else {
            rewind($this->stream);

            while ( ! feof($this->stream)) {
                $line = fgets($this->stream);

                if ( ! $line) {
                    continue;
                }

                yield json_decode($line, true);
            }
        }
    }

    public function count(): int
    {
        if ( ! isset($this->stream)) {
            return 0;
        }

        rewind($this->stream);

        $count = 0;

        while ( ! feof($this->stream)) {
            $line = fgets($this->stream);

            if ( ! $line) {
                continue;
            }

            $count++;
        }

        return $count;
    }

    public function first(): mixed
    {
        if ( ! isset($this->stream)) {
            return null;
        }

        rewind($this->stream);

        $line = fgets($this->stream);

        if ( ! $line) {
            return null;
        }

        return json_decode($line, true);
    }

    public function toJson(): string
    {
        if ( ! isset($this->stream)) {
            return '[]';
        }

        $buffer = '[';

        rewind($this->stream);

        while ( ! feof($this->stream)) {
            $line = fgets($this->stream);

            if ( ! $line) {
                continue;
            }

            $buffer .= strtr($line, [PHP_EOL => ', ']);
        }

        return mb_rtrim($buffer, ', ') . ']';
    }
}
