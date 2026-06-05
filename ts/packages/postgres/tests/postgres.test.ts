import { randomUUID } from "node:crypto";
import { Pool } from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { createPostgresDriver, type PostgresStoreDriver } from "../src/index.js";

const POSTGRES_URL = process.env.DOCKBAY_TEST_POSTGRES_URL;

describe.skipIf(POSTGRES_URL === undefined)("PostgresStoreDriver", () => {
  let pool: Pool;
  let table: string;
  let driver: PostgresStoreDriver;

  beforeAll(() => {
    table = `store_driver_test_${randomUUID().replaceAll("-", "_")}`;
    pool = new Pool({ connectionString: POSTGRES_URL });
    driver = createPostgresDriver(pool, { table });
  });

  afterAll(async () => {
    await pool.query(`DROP TABLE IF EXISTS "${table}"`);
    await driver.close();
  });

  it("upserts, scans, and admits exactly one CAS winner", async () => {
    await driver.transaction(async (txn) => {
      await txn.upsert("events", { runId: "run_1", seq: 2 }, { seq: 2 });
      await txn.upsert("events", { runId: "run_1", seq: 1 }, { seq: 1 });
      expect(await txn.get("events", { runId: "run_1", seq: 1 })).toEqual({ seq: 1 });
    });

    const scanned: unknown[] = [];
    await driver.transaction(async (txn) => {
      for await (const row of txn.scan("events", { runId: "run_1" })) {
        scanned.push(row);
      }
    });
    expect(scanned).toEqual([{ seq: 1 }, { seq: 2 }]);

    await driver.transaction((txn) =>
      txn.compareAndApply("locks", { id: "budget" }, undefined, { owner: "seed" }),
    );
    const results = await Promise.all(
      Array.from({ length: 10 }, (_, index) =>
        driver.transaction((txn) =>
          txn.compareAndApply("locks", { id: "budget" }, { owner: "seed" }, { owner: index }),
        ),
      ),
    );
    expect(results.filter(Boolean)).toHaveLength(1);
  });
});
