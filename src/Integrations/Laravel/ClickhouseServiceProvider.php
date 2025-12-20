<?php

declare(strict_types=1);

namespace Flarme\PhpClickhouse\Integrations\Laravel;

use Illuminate\Contracts\Foundation\Application;
use Illuminate\Contracts\Support\DeferrableProvider;
use Illuminate\Support\ServiceProvider;

class ClickhouseServiceProvider extends ServiceProvider implements DeferrableProvider
{
    /**
     * Register the service provider.
     */
    public function register(): void
    {
        $this->mergeConfigFrom(
            __DIR__ . '/config/clickhouse.php',
            'clickhouse'
        );

        $this->app->singleton(ClickhouseManager::class, fn(Application $app) => new ClickhouseManager($app));

        $this->app->singleton('clickhouse', fn($app) => new ClickhouseManager($app));

        $this->app->bind('clickhouse.connection', fn($app) => $app['clickhouse']->connection());
        $this->app->bind('clickhouse.schema', fn($app) => $app['clickhouse']->connection()->schema());
        $this->app->bind('clickhouse.query', fn($app) => $app['clickhouse']->connection()->query());
    }

    /**
     * Bootstrap the application services.
     */
    public function boot(): void
    {
        if ($this->app->runningInConsole()) {
            $this->publishes([
                __DIR__ . '/config/clickhouse.php' => $this->app->configPath('clickhouse.php'),
            ], 'clickhouse-config');
        }
    }
}
