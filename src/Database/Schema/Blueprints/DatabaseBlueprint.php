<?php

declare(strict_types=1);

namespace Flarme\PhpClickhouse\Database\Schema\Blueprints;

class DatabaseBlueprint extends Blueprint
{
    protected ?string $engine = null;

    public function getEngine(): ?string
    {
        return $this->engine;
    }

    /**
     * Set the database engine.
     */
    public function engine(string $engine): self
    {
        $this->engine = $engine;

        return $this;
    }

    /**
     * Use Atomic engine.
     */
    public function atomic(): self
    {
        return $this->engine('Atomic');
    }

    /**
     * Use Ordinary engine (deprecated).
     */
    public function ordinary(): self
    {
        return $this->engine('Ordinary');
    }

    /**
     * Use Lazy engine.
     */
    public function lazy(int $expirationTimeSeconds = 3600): self
    {
        $this->engine = "Lazy({$expirationTimeSeconds})";

        return $this;
    }

    /**
     * Use Replicated engine.
     */
    public function replicated(string $zkPath, string $shardName, string $replicaName): self
    {
        $this->engine = "Replicated('{$zkPath}', '{$shardName}', '{$replicaName}')";

        return $this;
    }
}
