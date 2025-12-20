<?php

declare(strict_types=1);

return [

    /*
    |--------------------------------------------------------------------------
    | Default ClickHouse Connection
    |--------------------------------------------------------------------------
    |
    | This option controls the default ClickHouse connection that will be used
    | when using the ClickHouse client. You may set this to any of the
    | connections defined in the "connections" array below.
    |
    */

    'default' => env('CLICKHOUSE_CONNECTION', 'default'),

    /*
    |--------------------------------------------------------------------------
    | ClickHouse Connections
    |--------------------------------------------------------------------------
    |
    | Here you may configure the connection information for each ClickHouse
    | server that is used by your application. You can configure as many
    | connections as you need.
    |
    */

    'connections' => [

        'default' => [
            'host' => env('CLICKHOUSE_HOST', 'localhost'),
            'port' => env('CLICKHOUSE_PORT', 8123),
            'username' => env('CLICKHOUSE_USERNAME', 'default'),
            'password' => env('CLICKHOUSE_PASSWORD', ''),
            'database' => env('CLICKHOUSE_DATABASE'),
            'secure' => env('CLICKHOUSE_SECURE', false),

            /*
            |--------------------------------------------------------------------------
            | Guzzle HTTP Options
            |--------------------------------------------------------------------------
            |
            | Additional options to pass to the Guzzle HTTP client.
            |
            | @see https://docs.guzzlephp.org/en/stable/request-options.html
            |
            */

            'options' => [
                'timeout' => env('CLICKHOUSE_TIMEOUT', 30),
                'connect_timeout' => env('CLICKHOUSE_CONNECT_TIMEOUT', 5),
            ],

            /*
            |--------------------------------------------------------------------------
            | ClickHouse Settings
            |--------------------------------------------------------------------------
            |
            | ClickHouse query settings to apply to all queries.
            |
            | @see https://clickhouse.com/docs/operations/settings/settings
            |
            */

            'settings' => [
                // 'max_execution_time' => 60,
                // 'max_memory_usage' => 10000000000,
            ],
        ],

    ],

];
