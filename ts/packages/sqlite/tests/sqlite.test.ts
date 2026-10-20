import { mkdtemp, rm } from "node:fs/promises";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createSQLiteDriver, type SQLiteDatabase } from "../src/index.js";

const isBun = "Bun" in globalThis;
const bunTest = "bun:test";
const { describe, expect, it } = isBun
  ? ((await import(bunTest)) as typeof import("vitest"))
  : await import("vitest");

async function openDatabase(path = ":memory:"): Promise<SQLiteDatabase> {
  if (isBun) {
    const moduleName = "bun:sqlite";
    const { Database } = (await import(moduleName)) as {
      Database: new (path: string) => SQLiteDatabase;
    };
    return new Database(path);
  }
  const { DatabaseSync } = createRequire(import.meta.url)(
    "node:sqlite",
  ) as typeof import("node:sqlite");
  return new DatabaseSync(path);
}

describe("SQLite store driver", () => {
  it("preserves the original SQLite error after an automatic rollback", async () => {
    const directory = await mkdtemp(join(tmpdir(), "dockbay-sqlite-"));
    const database = await openDatabase(join(directory, "rows.db"));
    try {
      for (const source of [join(directory, "rows.db"), database]) {
        const driver = createSQLiteDriver(source);
        try {
          await driver.transaction(async () => undefined);
          database.exec(`CREATE TRIGGER IF NOT EXISTS reject_write BEFORE INSERT ON store_driver_rows
            BEGIN SELECT RAISE(ROLLBACK, 'transaction aborted'); END`);
          let originalError: unknown;
          const result = await driver
            .transaction(async (txn) => {
              try {
                await txn.upsert("items", { id: 1 }, { count: 1 });
              } catch (error) {
                originalError = error;
                throw error;
              }
            })
            .catch((error: unknown) => error);
          expect(originalError).toMatchObject({ message: "transaction aborted" });
          expect(result).toBe(originalError);
        } finally {
          await driver.close();
        }
      }
    } finally {
      database.close();
      await rm(directory, { recursive: true, force: true });
    }
  });

  it("round-trips rows and scans a namespace in key order with pagination", async () => {
    const directory = await mkdtemp(join(tmpdir(), "dockbay-sqlite-"));
    const path = join(directory, "rows.db");
    let driver = createSQLiteDriver(path);
    try {
      await driver.transaction(async (txn) => {
        await txn.upsert("events", { scope: "a", seq: 2 }, { seq: 0 });
        await txn.upsert("events", { seq: 2, scope: "a" }, { seq: 2 });
        await txn.upsert("events", { scope: "a", seq: 1 }, { seq: 1 });
        await txn.upsert("events", { scope: "a", seq: 3 }, { seq: 3 });
        await txn.upsert("events", { scope: "b", seq: 1 }, { seq: 99 });
        await txn.upsert("other", { scope: "a", seq: 1 }, { seq: 99 });
        expect(await txn.get("events", { seq: 2, scope: "a" })).toEqual({ seq: 2 });
        expect(await txn.get("events", { seq: 99 })).toBeUndefined();
        const rows = [];
        for await (const row of txn.scan("events", { scope: "a" })) rows.push(row);
        expect(rows).toEqual([{ seq: 1 }, { seq: 2 }, { seq: 3 }]);
        const page = [];
        for await (const row of txn.scan(
          "events",
          { scope: "a" },
          {
            after: { scope: "a", seq: 1 },
            limit: 1,
          },
        ))
          page.push(row);
        expect(page).toEqual([{ seq: 2 }]);
      });
      await driver.close();
      driver = createSQLiteDriver(path);
      expect(await driver.transaction((txn) => txn.get("events", { scope: "a", seq: 2 }))).toEqual({
        seq: 2,
      });
      await driver.close();
      await expect(driver.transaction((txn) => txn.get("events", {}))).rejects.toThrow(
        "Store driver is closed",
      );
    } finally {
      await driver.close();
      await rm(directory, { recursive: true, force: true });
    }
  });

  it("applies matching values and leaves rows unchanged on conflicts", async () => {
    const driver = createSQLiteDriver(":memory:");
    try {
      await driver.transaction(async (txn) => {
        const key = { id: "state" };
        expect(await txn.compareAndApply("state", key, null, 7)).toBe(false);
        expect(await txn.compareAndApply("state", key, undefined, { z: 1, a: 2 })).toBe(true);
        expect(await txn.compareAndApply("state", key, undefined, 99)).toBe(false);
        expect(await txn.compareAndApply("state", key, { a: 2, z: 1 }, 7)).toBe(true);
        expect(await txn.get("state", key)).toEqual({ value: 7 });
        expect(await txn.compareAndApply("state", key, 8, 99)).toBe(false);
        expect(await txn.compareAndApply("state", key, 7, null)).toBe(true);
        expect(await txn.compareAndApply("state", key, null, [1, 2])).toBe(true);
        expect(await txn.get("state", key)).toEqual({ value: [1, 2] });
        await txn.upsert("state", key, { replaced: true });
        expect(await txn.compareAndApply("state", key, { replaced: true }, { done: true })).toBe(
          true,
        );
      });
      const winners = await Promise.all(
        [1, 2].map((owner) =>
          driver.transaction((txn) =>
            txn.compareAndApply("state", { id: "state" }, { done: true }, { owner }),
          ),
        ),
      );
      expect(winners).toEqual([true, false]);
      expect(await driver.transaction((txn) => txn.get("state", { id: "state" }))).toEqual({
        owner: 1,
      });
    } finally {
      await driver.close();
    }
  });

  it("shares a caller transaction and rolls back driver work without closing the connection", async () => {
    const database = await openDatabase();
    const driver = createSQLiteDriver(database);
    try {
      database.exec("CREATE TABLE own_rows (id TEXT); BEGIN; INSERT INTO own_rows VALUES ('a')");
      await driver.transaction((txn) => txn.upsert("events", { id: 1 }, { saved: true }));
      await expect(
        driver.transaction(async (txn) => {
          await txn.upsert("events", { id: 2 }, { saved: false });
          throw new Error("abort");
        }),
      ).rejects.toThrow("abort");
      expect(await driver.transaction((txn) => txn.get("events", { id: 1 }))).toEqual({
        saved: true,
      });
      expect(await driver.transaction((txn) => txn.get("events", { id: 2 }))).toBeUndefined();
      database.exec("ROLLBACK");
      expect(database.prepare("SELECT * FROM own_rows").all()).toEqual([]);
      expect(await driver.transaction((txn) => txn.get("events", { id: 1 }))).toBeUndefined();
      await driver.close();
      database.exec("INSERT INTO own_rows VALUES ('still open')");
      expect(database.prepare("SELECT * FROM own_rows").all()).toEqual([{ id: "still open" }]);
    } finally {
      await driver.close();
      database.close();
    }
  });
});
