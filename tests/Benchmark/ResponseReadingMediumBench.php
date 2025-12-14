<?php

declare(strict_types=1);

namespace Flarme\PhpClickhouse\Tests\Benchmark;

use Tests\Benchmark\AbstractReadingBench;

class ResponseReadingMediumBench extends AbstractReadingBench
{
    protected int $count = 1000;
}
