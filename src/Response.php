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
        $this->summary = json_decode($response->getHeaderLine('x-clickhouse-summary'), true);
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
        return iterator_to_array($this->rows());
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
        rewind($this->stream);

        $content = stream_get_contents($this->stream);

        if ( ! $content || ! mb_strlen($content)) {
            return 0;
        }

        return mb_substr_count($content, PHP_EOL) + 1;
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

        rewind($this->stream);

        $content = stream_get_contents($this->stream);

        if ( ! mb_strlen($content)) {
            return '[]';
        }

        return '[' . mb_rtrim(
            // Replace new lines with comma and remove tabs
            strtr(
                $content,
                [PHP_EOL => ',', '    {' => '{'],
            ),
            ',',
        ) . ']';
    }
}
