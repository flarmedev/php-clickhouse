<?php

declare(strict_types=1);

namespace Flarme\PhpClickhouse;

use Flarme\PhpClickhouse\Contracts\ClientInterface;
use Flarme\PhpClickhouse\Contracts\QueryInterface;
use Flarme\PhpClickhouse\Contracts\ResponseInterface;
use Flarme\PhpClickhouse\Exceptions\ClickhouseException;
use Flarme\PhpClickhouse\Exceptions\UnsupportedBindingException;
use GuzzleHttp\Client as HttpClient;
use GuzzleHttp\Exception\GuzzleException;

class Client implements ClientInterface
{
    private ?string $database;

    private HttpClient $httpClient;

    /**
     * @param  array  $options  Guzzle options
     * @param  array  $settings  Clickhouse settings
     *
     * @see https://docs.guzzlephp.org/en/stable/request-options.html
     * @see https://clickhouse.com/docs/operations/settings/settings
     */
    public function __construct(
        string $host,
        int $port,
        string $username,
        string $password,
        ?string $database = null,
        bool $secure = false,
        array $options = [],
        array $settings = [],
    ) {
        $protocol = $secure ? 'https' : 'http';

        $configuration = [
            'base_uri' => "{$protocol}://{$host}:{$port}",
            'auth' => [$username, $password],
            'query' => $settings,
            'connect_timeout' => 5,
            ...$options,
        ];

        $this->httpClient = new HttpClient($configuration);

        $this->database = $database;
    }

    /**
     * @return $this
     */
    public function database(string $database): self
    {
        $this->database = $database;

        return $this;
    }

    /**
     * @throws ClickhouseException`
     * @throws UnsupportedBindingException
     */
    public function execute(string|QueryInterface $query, array $bindings = []): ResponseInterface
    {
        if (is_string($query)) {
            $query = Query::from($query, $bindings);
        }

        try {
            $response = $this->httpClient->post('/', [
                'headers' => [
                    'x-clickhouse-database' => $this->database,
                    'x-clickhouse-format' => 'NDJSON',
                ],
                'multipart' => $query->toMultipart(),
            ]);
        } catch (GuzzleException $e) {
            throw new ClickhouseException($e->getMessage(), $e->getCode(), $e);
        }

        return new Response($response);
    }
}
