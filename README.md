# PHP ClickHouse

A modern, fluent, and high-performance ClickHouse driver for PHP.

[![Tests](https://github.com/flarmedev/php-clickhouse/workflows/Tests/badge.svg)](https://github.com/flarmedev/php-clickhouse/actions)
[![Latest Version](https://img.shields.io/packagist/v/flarmedev/php-clickhouse.svg)](https://packagist.org/packages/flarmedev/php-clickhouse)
[![License](https://img.shields.io/packagist/l/flarmedev/php-clickhouse)](LICENSE)

## Features

- **Fluent Query Builder** — Build complex queries with an expressive, chainable API
- **Schema Builder** — Manage tables, views, materialized views, and dictionaries
- **High Performance** — Stream-based response processing and optimized batch inserts
- **ClickHouse Native** — Full support for `PREWHERE`, `FINAL`, `SAMPLE`, `ARRAY JOIN`, `LIMIT BY`, window functions,
  and more

## Requirements

- PHP 8.4+
- ClickHouse 21.8+

## Installation

```bash
composer require flarmedev/php-clickhouse
```

## Quick Start

### Connect to ClickHouse

```php
use Flarme\PhpClickhouse\Client;

$client = new Client(
    host: 'localhost',
    port: 8123,
    username: 'default',
    password: '',
    database: 'default'
);
```

### Query Data

```php
use Flarme\PhpClickhouse\Database\Query\Builder;

$users = Builder::query($client)
    ->select('id', 'name', 'email')
    ->from('users')
    ->where('status', '=', 'active')
    ->orderByDesc('created_at')
    ->limit(10)
    ->get();
```

### Insert Data

```php
$client->insert('users', [
    ['id' => 1, 'name' => 'Alice', 'email' => 'alice@example.com'],
    ['id' => 2, 'name' => 'Bob', 'email' => 'bob@example.com'],
]);
```

### Create Tables

```php
use Flarme\PhpClickhouse\Database\Schema\Builder as Schema;

$schema = new Schema($client);

$schema->create('events', function ($table) {
    $table->uint64('id');
    $table->string('event_type');
    $table->dateTime('created_at');
    $table->mergeTree();
    $table->orderBy('id');
});
```

## Documentation

Full documentation is available at **[php-clickhouse.flarme.com](https://php-clickhouse.flarme.com)**.

## Contributing

Contributions are welcome! Please read our [Contributing Guide](.github/CONTRIBUTING.md) before submitting a pull
request.

## Security

If you discover a security vulnerability, please email [contact@flarme.com](mailto:contact@flarme.com). See
our [Security Policy](.github/SECURITY.md) for more details.

## License

MIT License. See [LICENSE](LICENSE) for details.
