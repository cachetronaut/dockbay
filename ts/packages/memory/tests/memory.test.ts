import { describe, expect, it } from "vitest";
import { createInMemoryDriver } from "../src/index.js";

describe("in-memory store driver", () => {
  it("upserts idempotently and gets rows by primitive-owned keys", async () => {
    const driver = createInMemoryDriver();

    await driver.transaction(async (txn) => {
      await txn.upsert("events", { runId: "run_1", seq: 1 }, { type: "stage" });
      await txn.upsert("events", { runId: "run_1", seq: 1 }, { type: "stage" });
      expect(await txn.get("events", { runId: "run_1", seq: 1 })).toEqual({ type: "stage" });
    });
  });

  it("scans rows in key order by prefix", async () => {
    const driver = createInMemoryDriver();

    await driver.transaction(async (txn) => {
      await txn.upsert("events", { runId: "run_1", seq: 2 }, { seq: 2 });
      await txn.upsert("events", { runId: "run_2", seq: 1 }, { seq: 1 });
      await txn.upsert("events", { runId: "run_1", seq: 1 }, { seq: 1 });
      const rows = [];
      for await (const row of txn.scan("events", { runId: "run_1" })) {
        rows.push(row);
      }
      expect(rows).toEqual([{ seq: 1 }, { seq: 2 }]);
    });
  });

  it("admits exactly one compare-and-apply winner under contention", async () => {
    const driver = createInMemoryDriver();

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
