<?php

declare(strict_types=1);

namespace Flarme\PhpClickhouse\Tests\Benchmark;

use Flarme\PhpClickhouse\Query;
use PhpBench\Attributes\Iterations;
use PhpBench\Attributes\Revs;

#[Revs(1000), Iterations(10)]
class QueryCompilationBench
{
    public function benchQueryCompilation(): void
    {
        $sql = 'SELECT * FROM users WHERE id = ? AND name = :name AND status IN (?, ?, ?) AND created_at > {date}';
        $bindings = [
            0 => 1,
            'name' => 'John',
            1 => 'active',
            2 => 'pending',
            3 => 'verified',
            'date' => '2024-01-01',
        ];

        $query = new Query($sql, $bindings);

        $multipart = $query->toMultipart();
    }
}
