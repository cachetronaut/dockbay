# DockBay TS

DockBay is the TypeScript backend driver substrate for agent-fabric store
adapters. It defines a small transactional key-value row contract and ships
memory, Convex-operation, and Postgres implementations.

This repo publishes the unscoped npm package `dockbay`.

## Install

```sh
pnpm add dockbay
```

## API

The TypeScript and Python packages expose the same concepts:

- `Row`
- `ScanOptions`
- `Transaction`
- `StoreDriver`
- `MigrationSet`
- `canonicalize`
- `keyOf`
- `matchesPrefix`
- `compareRows`
- in-memory driver
- Convex operation host and operation driver
- Postgres driver

TypeScript exposes core types from `dockbay` and adapters from subpaths:

- `dockbay`
- `dockbay/memory`
- `dockbay/convex`
- `dockbay/postgres`

Python exposes the same concepts from the `dockbay` package root with snake_case
function names.

## Usage

```ts
import { createInMemoryDriver } from "dockbay/memory";

const driver = createInMemoryDriver();

await driver.transaction(async (txn) => {
  await txn.upsert(
    "runs",
    { tenantId: "tenant_demo", runId: "run_001" },
    { tenantId: "tenant_demo", runId: "run_001", status: "ok" },
  );

  const row = await txn.get("runs", {
    tenantId: "tenant_demo",
    runId: "run_001",
  });

  console.log(row);
});

await driver.close();
```

## Postgres

```ts
import { Pool } from "pg";
import { createPostgresDriver } from "dockbay/postgres";

const pool = new Pool({ connectionString: process.env.DOCKBAY_TEST_POSTGRES_URL });
const driver = createPostgresDriver(pool, { table: "store_driver_rows" });
```

## Mirror Contract

`dockbay` and `dockbay` must preserve the same behavior:

- `upsert` writes a row by table and canonical key.
- `get` reads a row by table and canonical key.
- `scan` returns rows whose keys match a prefix in canonical key order.
- `scan` supports `after` and `limit`.
- `compareAndApply` / `compare_and_apply` updates only when the stored value
  matches the expected value.
- `canonicalize` sorts object keys and omits `undefined` in TypeScript.

## Development

```sh
pnpm install --frozen-lockfile
pnpm verify
pnpm build
npm pack --dry-run
```

Postgres tests run when `DOCKBAY_TEST_POSTGRES_URL` is set.
