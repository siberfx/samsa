# Contributing

Thanks for helping improve Samsa! Issues and pull requests are welcome at
[github.com/siberfx/samsa](https://github.com/siberfx/samsa).

## Before you start

- For anything bigger than a small fix, open an issue first so we can agree on the approach.
- Samsa's guarantees (per-key ordering, at-least-once delivery, fencing) are its core. Changes that touch the drivers or the worker need tests that prove those guarantees still hold.

## Development

```bash
composer install
composer test        # PHPUnit on SQLite
composer lint        # Laravel Pint
composer analyse     # PHPStan
```

Run the suite against real servers, including the multi-process tests:

```bash
docker compose up -d mysql postgres
SAMSA_TEST_DB_CONNECTION=mysql DB_DATABASE=samsa_test DB_USERNAME=root vendor/bin/phpunit
SAMSA_TEST_DB_CONNECTION=pgsql DB_PORT=5433 DB_DATABASE=samsa_test DB_USERNAME=postgres DB_PASSWORD=postgres vendor/bin/phpunit
```

## Pull requests

- Add or update tests for every behaviour change. New drivers must pass `Siberfx\Samsa\Testing\StorageDriverContract`.
- Update `documentation/` and add an entry under `[Unreleased]` in `CHANGELOG.md`.
- Keep the public API backwards compatible within a major version. Classes marked `@internal` may change.
- CI (lint, static analysis, the PHP × Laravel matrix and the MySQL/PostgreSQL jobs) must pass.
