# dockbay

Python implementation of DockBay.

For product-level context, shared contracts, and cross-language repository information, see the public repository: https://github.com/cachetronaut/dockbay.

## Install

```sh
pip install dockbay
```

## Import

```python
import dockbay
```

## Read-only snapshots and SQLite projections

Use `dockbay.ReadOnlySQLiteStoreDriver(path)` to read an existing database without
initializing it. Missing, empty, and non-DockBay databases raise
`ReadOnlyStoreUnavailable`. Each transaction starts a SQLite read snapshot, and
logical writes raise `PermissionError`. The connection also uses SQLite read-only
mode and `query_only`.

`await driver.with_connection(work)` calls `await work(transaction, connection)`
under the driver's transaction lock. Logical row writes and application SQL use
the same connection and commit or roll back together. The caller must not retain,
commit, roll back, or close the supplied connection. Keep the driver and callbacks
on the same owning thread. Read-only drivers support the callback for queries;
they retain the same write restrictions.

## Development

Run from `py/`:

```sh
uv sync --dev
uv run --with ruff ruff check .
uv run --with ruff ruff format --check .
uv run --with ty ty check
uv run --with pytest --with pytest-asyncio python -m pytest
```

## License

MIT
