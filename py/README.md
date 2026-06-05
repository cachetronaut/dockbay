# DockBay Py

DockBay is the Python backend driver substrate for agent-fabric store adapters.
It defines a small transactional key-value row contract and ships memory,
Convex-operation, and Postgres implementations.

This repo publishes the PyPI project `dockbay`.

## Install

```sh
uv add dockbay
```

## API

The Python and TypeScript packages expose the same concepts:

- `Row`
- `ScanOptions`
- `Transaction`
- `StoreDriver`
- `MigrationSet`
- `canonicalize`
- `key_of`
- `matches_prefix`
- `compare_rows`
- in-memory driver
- Convex operation host and operation driver
- Postgres driver

Python exposes the package from `dockbay`. TypeScript exposes core types from
`dockbay` and adapters from subpaths such as `dockbay/memory`.

## Usage

```py
from __future__ import annotations

from dockbay import Transaction, create_in_memory_driver


async def main() -> None:
    driver = create_in_memory_driver()

    async def work(txn: Transaction) -> dict[str, object] | None:
        await txn.upsert(
            "runs",
            {"tenantId": "tenant_demo", "runId": "run_001"},
            {"tenantId": "tenant_demo", "runId": "run_001", "status": "ok"},
        )
        return await txn.get(
            "runs",
            {"tenantId": "tenant_demo", "runId": "run_001"},
        )

    row = await driver.transaction(work)
    print(row)
    await driver.close()
```

## Postgres

```py
import os

from psycopg_pool import AsyncConnectionPool

from dockbay import PostgresStoreDriverOptions, create_postgres_driver

pool = AsyncConnectionPool(os.environ["DOCKBAY_TEST_POSTGRES_URL"], open=False)
driver = create_postgres_driver(pool, PostgresStoreDriverOptions(table="store_driver_rows"))
```

## Mirror Contract

`dockbay` and `dockbay` must preserve the same behavior:

- `upsert` writes a row by table and canonical key.
- `get` reads a row by table and canonical key.
- `scan` returns rows whose keys match a prefix in canonical key order.
- `scan` supports `after` and `limit`.
- `compare_and_apply` / `compareAndApply` updates only when the stored value
  matches the expected value.
- `canonicalize` sorts object keys and emits compact JSON.

## Development

```sh
uv sync --dev
uv run ruff check .
uv run ruff format --check .
uv run ty check
uv run python -m pytest
uv build --out-dir dist --clear
```

Postgres tests run when `DOCKBAY_TEST_POSTGRES_URL` is set.
