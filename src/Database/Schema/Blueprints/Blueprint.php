<?php

declare(strict_types=1);

namespace Flarme\PhpClickhouse\Database\Schema\Blueprints;

use Flarme\PhpClickhouse\Database\Schema\Contracts\BlueprintInterface;

abstract class Blueprint implements BlueprintInterface
{
    protected string $name;

    protected bool $ifNotExists = false;

    protected ?string $onCluster = null;

    protected ?string $comment = null;

    public function __construct(string $name)
    {
        $this->name = $name;
    }

    public function getName(): string
    {
        return $this->name;
    }

    public function shouldUseIfNotExists(): bool
    {
        return $this->ifNotExists;
    }

    public function getOnCluster(): ?string
    {
        return $this->onCluster;
    }

    public function getComment(): ?string
    {
        return $this->comment;
    }

    /**
     * Use IF NOT EXISTS clause.
     */
    public function ifNotExists(bool $ifNotExists = true): self
    {
        $this->ifNotExists = $ifNotExists;

        return $this;
    }

    /**
     * Set the ON CLUSTER clause.
     */
    public function onCluster(string $cluster): self
    {
        $this->onCluster = $cluster;

        return $this;
    }

    /**
     * Set a comment.
     */
    public function comment(string $comment): self
    {
        $this->comment = $comment;

        return $this;
    }
}
