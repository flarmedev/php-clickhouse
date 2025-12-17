# Contributing

Thank you for your interest in contributing to PHP ClickHouse! This guide will help you get started.

## Code of Conduct

This project follows the [Contributor Covenant Code of Conduct](CODE_OF_CONDUCT.md). By participating, you agree to
uphold this code.

## How to Contribute

### Reporting Bugs

Open an issue on [GitHub](https://github.com/flarmedev/php-clickhouse/issues) with:

- A clear title and description
- Steps to reproduce the issue
- Expected vs actual behavior
- PHP version, ClickHouse version, and package version

### Suggesting Features

Open an issue to discuss your idea before implementing it. Include:

- A clear description of the feature
- Use cases and benefits
- Any implementation ideas you have

### Submitting Pull Requests

1. Fork the repository
2. Create a branch from `main`
3. Make your changes
4. Add tests for new functionality
5. Ensure all tests pass
6. Submit a pull request

## Development Setup

### Prerequisites

- PHP 8.4+
- Composer
- Docker (for running ClickHouse locally)

### Installation

```bash
git clone https://github.com/flarmedev/php-clickhouse.git
cd php-clickhouse
composer install
```

### Running ClickHouse

```bash
docker-compose up -d
```

### Running Tests

```bash
# All tests
composer test

# Unit tests only
composer test -- --testsuite=Unit

# Feature tests only
composer test -- --testsuite=Feature
```

### Code Style

This project uses [Laravel Pint](https://laravel.com/docs/pint) for code formatting:

```bash
# Check code style
composer lint

# Fix code style
composer format
```

### Static Analysis

```bash
composer analyse
```

## Pull Request Guidelines

- Keep changes focused and atomic
- Write clear commit messages
- Update documentation if needed
- Add tests for new features
- Ensure CI passes before requesting review

## Questions?

Open an issue or email [contact@flarme.com](mailto:contact@flarme.com).
