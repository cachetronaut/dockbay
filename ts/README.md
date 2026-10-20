---
status: current
updated: 2026-10-20
description: TypeScript package installation and SQLite store driver usage.
keywords: [typescript, sqlite, node, bun, store-driver]
---

# dockbay

TypeScript implementation of DockBay.

For shared contracts and cross-language repository information, see the public repository: https://github.com/cachetronaut/dockbay.

## Install

```sh
npm install dockbay
```

## SQLite store driver

Import `createSQLiteDriver` or `SQLiteStoreDriver` from `dockbay/sqlite`.
The driver uses `node:sqlite` on Node 24 or later and `bun:sqlite` on Bun.
It adds no native npm dependencies.

```ts
import { createSQLiteDriver } from "dockbay/sqlite";

const driver = createSQLiteDriver("store.db"); // Use ":memory:" for an in-memory database.
await driver.transaction(async (txn) => {
  await txn.upsert("items", { id: "one" }, { count: 1 });
  const row = await txn.get("items", { id: "one" });
});
await driver.close();
```

Pass an already-open `DatabaseSync` from `node:sqlite` or `Database` from
`bun:sqlite` to share a connection. Explicit adapters are also available as
`createNodeSQLiteConnection(database)` and `createBunSQLiteConnection(database)`.

```ts
import { DatabaseSync } from "node:sqlite";
import { createSQLiteDriver } from "dockbay/sqlite";

const database = new DatabaseSync(":memory:");
const driver = createSQLiteDriver(database);
database.exec("CREATE TABLE other_rows (id TEXT); BEGIN");
database.exec("INSERT INTO other_rows VALUES ('one')");
await driver.transaction((txn) => txn.upsert("items", { id: "one" }, { count: 1 }));
database.exec("ROLLBACK"); // Rolls back both writes.
await driver.close(); // Leaves the supplied connection open.
database.close();
```

Owned connections use WAL, foreign keys, and `BEGIN IMMEDIATE`. Supplied connections
retain their existing connection settings and use savepoints, including inside an
existing transaction. A successful callback commits or releases its savepoint;
a rejected callback rolls back its writes. The driver serializes its callbacks
and waits for them before closing. Await each callback before using the same
connection elsewhere, and keep transaction objects within their callback.

The default physical table is `store_driver_rows`; set `{ table: "custom_rows" }`
as the second factory argument to change it. Logical table names remain separate
namespaces. Keys and values use the TS core JSON codec, and scans use its key order,
prefix matching, exclusive `after`, and `limit` rules. `compareAndApply` atomically
compares the raw value; it returns `false` without changing a row on conflict.
Use `undefined` as the expected value to insert only when absent. Scalar, array,
and null values read back as `{ value: next }` and compare against the raw value.
JSON encoding omits undefined object properties; a top-level undefined CAS value
uses an empty `value_data` marker and reads back as `{}`.

## Development

Run from `ts/`:

```sh
pnpm install
pnpm verify
pnpm build
```

Run the SQLite tests under Bun with `bun test ./packages/sqlite/tests/sqlite.test.ts`.

## License

MIT
