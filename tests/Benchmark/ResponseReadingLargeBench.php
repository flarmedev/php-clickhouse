<?php

declare(strict_types=1);

namespace Flarme\PhpClickhouse\Tests\Benchmark;

use Tests\Benchmark\AbstractReadingBench;

class ResponseReadingLargeBench extends AbstractReadingBench
{
    protected int $count = 10000;
}
