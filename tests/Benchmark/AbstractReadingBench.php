<?php

declare(strict_types=1);

namespace Tests\Benchmark;

use Flarme\PhpClickhouse\Client;
use Flarme\PhpClickhouse\Response;
use PhpBench\Attributes\Iterations;
use PhpBench\Attributes\Revs;

#[Revs(1000), Iterations(10)]
abstract class AbstractReadingBench
{
    abstract protected int $count {
        get;
    }

    protected Response $response;

    public function __construct()
    {
        $client = new Client(
            host: $_ENV['CLICKHOUSE_HOST'] ?? 'localhost',
            port: (int) ($_ENV['CLICKHOUSE_PORT'] ?? 8123),
            username: $_ENV['CLICKHOUSE_USERNAME'] ?? 'user',
            password: $_ENV['CLICKHOUSE_PASSWORD'] ?? 'password',
            database: $database ?? $_ENV['CLICKHOUSE_DATABASE'] ?? 'database',
        );

        $this->response = $client->execute("SELECT number, 'REF-1T8H' as ref, 789.23 as amount, true as bool, toDateTime('2023-01-02 03:46:41') as created_at FROM system.numbers LIMIT {$this->count};");
    }

    public function benchToArray(): void
    {
        $this->response->toArray();
    }

    public function benchRows(): void
    {
        foreach ($this->response->rows() as $row) {
        }
    }

    public function benchFirst(): void
    {
        $this->response->first();
    }

    public function benchCount(): void
    {
        $this->response->count();
    }

    public function benchToJson(): void
    {
        $this->response->toJson();
    }
}
