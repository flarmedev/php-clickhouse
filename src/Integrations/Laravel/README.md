# Laravel Integration

This package provides seamless Laravel integration for PHP ClickHouse.

## Configuration

Publish the configuration file:

```bash
php artisan vendor:publish --tag=clickhouse-config
```

Configure your connection in `.env`:

```env
CLICKHOUSE_HOST=localhost
CLICKHOUSE_PORT=8123
CLICKHOUSE_USERNAME=default
CLICKHOUSE_PASSWORD=
CLICKHOUSE_DATABASE=default
CLICKHOUSE_SECURE=false
```

## Usage

### Using the Facade

```php
use Flarme\PhpClickhouse\Integrations\Laravel\Facades\Clickhouse as Clickhouse;

// Execute a query
$result = Clickhouse::execute('SELECT * FROM users LIMIT 10');

// Use the query builder
$users = Clickhouse::query()
    ->from('users')
    ->where('status', '=', 'active')
    ->get();

// Insert data
Clickhouse::insert('users', [
    ['id' => 1, 'name' => 'Alice'],
    ['id' => 2, 'name' => 'Bob'],
]);

// Use the schema builder
Clickhouse::schema()->create('events', function ($table) {
    $table->uint64('id');
    $table->string('name');
    $table->mergeTree();
    $table->orderBy('id');
});
```

### Using Dependency Injection

```php
use Flarme\PhpClickhouse\Client;

class UserController extends Controller
{
    public function __construct(
        private Client $clickhouse
    ) {}

    public function index()
    {
        return $this->clickhouse
            ->query()
            ->from('users')
            ->get();
    }
}
```

### Multiple Connections

Configure multiple connections in `config/clickhouse.php`:

```php
'connections' => [
    'default' => [
        'host' => env('CLICKHOUSE_HOST', 'localhost'),
        // ...
    ],
    'analytics' => [
        'host' => env('CLICKHOUSE_ANALYTICS_HOST', 'analytics.example.com'),
        // ...
    ],
],
```

Use a specific connection:

```php
// Via facade
$result = Clickhouse::connection('analytics')->execute('SELECT 1');

// Or set the default
Clickhouse::setDefaultConnection('analytics');
```

### Using the Manager

```php
use Flarme\PhpClickhouse\Integrations\Laravel\ClickhouseManager;

class AnalyticsService
{
    public function __construct(
        private ClickhouseManager $clickhouse
    ) {}

    public function getStats()
    {
        return $this->clickhouse
            ->connection('analytics')
            ->query()
            ->from('events')
            ->get();
    }
}
```
