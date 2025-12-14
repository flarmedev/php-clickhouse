<?php

declare(strict_types=1);

namespace Flarme\PhpClickhouse\Database\Schema\Contracts;

interface BlueprintInterface
{
    /**
     * Get the name of the object being defined.
     */
    public function getName(): string;

    /**
     * Check if the blueprint should use IF NOT EXISTS.
     */
    public function shouldUseIfNotExists(): bool;

    /**
     * Get the ON CLUSTER clause if set.
     */
    public function getOnCluster(): ?string;
}
