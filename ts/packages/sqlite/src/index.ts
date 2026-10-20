import type { Row, ScanOptions, StoreDriver, Transaction } from "@dockbay/core";
import { canonicalize, compareRows, keyOf, matchesPrefix } from "@dockbay/core";
import { createBunSQLiteConnection, openBunSQLiteConnection } from "./bun.js";
import type { SQLiteConnection, SQLiteDatabase } from "./connection.js";
import { createNodeSQLiteConnection, openNodeSQLiteConnection } from "./node.js";

export { createBunSQLiteConnection } from "./bun.js";
export type { SQLiteConnection, SQLiteDatabase, SQLiteStatement } from "./connection.js";
export { createNodeSQLiteConnection } from "./node.js";

export interface SQLiteStoreDriverOptions {
  readonly table?: string;
}

type SQLiteSource = string | SQLiteDatabase | SQLiteConnection;
const IDENTIFIER = /^[A-Za-z_][A-Za-z0-9_]*$/;
const RESERVED = new Set(
  `ABORT ACTION ADD AFTER ALL ALTER ALWAYS ANALYZE AND AS ASC ATTACH AUTOINCREMENT BEFORE
   BEGIN BETWEEN BY CASCADE CASE CAST CHECK COLLATE COLUMN COMMIT CONFLICT CONSTRAINT CREATE
   CROSS CURRENT CURRENT_DATE CURRENT_TIME CURRENT_TIMESTAMP DATABASE DEFAULT DEFERRABLE
   DEFERRED DELETE DESC DETACH DISTINCT DO DROP EACH ELSE END ESCAPE EXCEPT EXCLUDE EXCLUSIVE
   EXISTS EXPLAIN FAIL FILTER FIRST FOLLOWING FOR FOREIGN FROM FULL GENERATED GLOB GROUP GROUPS
   HAVING IF IGNORE IMMEDIATE IN INDEX INDEXED INITIALLY INNER INSERT INSTEAD INTERSECT INTO IS
   ISNULL JOIN KEY LAST LEFT LIKE LIMIT MATCH MATERIALIZED NATURAL NO NOT NOTHING NOTNULL NULL
   NULLS OF OFFSET ON OR ORDER OTHERS OUTER OVER PARTITION PLAN PRAGMA PRECEDING PRIMARY QUERY
   RAISE RANGE RECURSIVE REFERENCES REGEXP REINDEX RELEASE RENAME REPLACE RESTRICT RETURNING
   RIGHT ROLLBACK ROW ROWS SAVEPOINT SELECT SET TABLE TEMP TEMPORARY THEN TIES TO TRANSACTION
   TRIGGER UNBOUNDED UNION UNIQUE UPDATE USING VACUUM VALUES VIEW VIRTUAL WHEN WHERE WINDOW
   WITH WITHOUT`.split(/\s+/),
);
let savepointId = 0;

export class SQLiteStoreDriver implements StoreDriver {
  readonly backend = "sqlite";
  private readonly table: string;
  private connection: SQLiteConnection | undefined;
  private queue = Promise.resolve();
  private closed = false;

  constructor(
    private readonly source: SQLiteSource,
    options: SQLiteStoreDriverOptions = {},
  ) {
    const table = options.table ?? "store_driver_rows";
    if (!IDENTIFIER.test(table)) {
      throw new Error(`Invalid SQLite store-driver table: ${table}`);
    }
    if (RESERVED.has(table.toUpperCase())) {
      throw new Error(`SQLite reserved word cannot be used as table name: ${table}`);
    }
    this.table = `"${table}"`;
  }

  transaction<T>(work: (txn: Transaction) => Promise<T>): Promise<T> {
    return this.enqueue(async () => {
      if (this.closed) {
        throw new Error("Store driver is closed");
      }
      const connection = await this.ensureOpen();
      const savepoint = typeof this.source === "string" ? undefined : `dockbay_${++savepointId}`;
      connection.exec(savepoint === undefined ? "BEGIN IMMEDIATE" : `SAVEPOINT ${savepoint}`);
      try {
        connection.exec(`CREATE TABLE IF NOT EXISTS ${this.table} (
          table_name TEXT NOT NULL,
          key_json TEXT NOT NULL,
          key_json_data TEXT NOT NULL,
          row_data TEXT NOT NULL,
          value_data TEXT NOT NULL,
          PRIMARY KEY (table_name, key_json)
        )`);
        const txn = new SQLiteTransaction(connection, this.table);
        try {
          const result = await work(txn);
          connection.exec(savepoint === undefined ? "COMMIT" : `RELEASE SAVEPOINT ${savepoint}`);
          return result;
        } finally {
          txn.finish();
        }
      } catch (error) {
        if (savepoint === undefined) {
          connection.exec("ROLLBACK");
        } else {
          connection.exec(`ROLLBACK TO SAVEPOINT ${savepoint}`);
          connection.exec(`RELEASE SAVEPOINT ${savepoint}`);
        }
        throw error;
      }
    });
  }

  close(): Promise<void> {
    return this.enqueue(async () => {
      if (this.closed) {
        return;
      }
      this.closed = true;
      if (typeof this.source === "string") {
        this.connection?.close();
      }
      this.connection = undefined;
    });
  }

  private enqueue<T>(work: () => Promise<T>): Promise<T> {
    const next = this.queue.then(work);
    this.queue = next.then(
      () => undefined,
      () => undefined,
    );
    return next;
  }

  private async ensureOpen(): Promise<SQLiteConnection> {
    if (this.connection !== undefined) {
      return this.connection;
    }
    if (typeof this.source !== "string") {
      this.connection =
        "prepare" in this.source
          ? isBun()
            ? createBunSQLiteConnection(this.source)
            : createNodeSQLiteConnection(this.source)
          : this.source;
      return this.connection;
    }
    const connection = await (isBun()
      ? openBunSQLiteConnection(this.source)
      : openNodeSQLiteConnection(this.source));
    try {
      connection.exec("PRAGMA journal_mode=WAL");
      connection.exec("PRAGMA foreign_keys=ON");
    } catch (error) {
      connection.close();
      throw error;
    }
    this.connection = connection;
    return connection;
  }
}

export function createSQLiteDriver(
  source: SQLiteSource,
  options: SQLiteStoreDriverOptions = {},
): SQLiteStoreDriver {
  return new SQLiteStoreDriver(source, options);
}

class SQLiteTransaction implements Transaction {
  private active = true;

  constructor(
    private readonly connection: SQLiteConnection,
    private readonly table: string,
  ) {}

  finish(): void {
    this.active = false;
  }

  async upsert(table: string, key: Row, row: Row): Promise<void> {
    this.ensureActive();
    const keyJson = keyOf(key);
    const rowJson = canonicalize(row);
    this.connection.run(
      `INSERT INTO ${this.table} (table_name, key_json, key_json_data, row_data, value_data)
       VALUES (?, ?, ?, ?, ?)
       ON CONFLICT (table_name, key_json) DO UPDATE SET
       key_json_data = excluded.key_json_data, row_data = excluded.row_data,
       value_data = excluded.value_data`,
      table,
      keyJson,
      keyJson,
      rowJson,
      rowJson,
    );
  }

  async get(table: string, key: Row): Promise<Row | undefined> {
    this.ensureActive();
    const result = this.connection.get(
      `SELECT row_data FROM ${this.table} WHERE table_name = ? AND key_json = ?`,
      table,
      keyOf(key),
    );
    return result == null ? undefined : decodeRow(result, "row_data");
  }

  async *scan(table: string, prefix: Row, opts: ScanOptions = {}): AsyncIterable<Row> {
    this.ensureActive();
    const entries = this.connection
      .all(`SELECT key_json_data, row_data FROM ${this.table} WHERE table_name = ?`, table)
      .map((entry) => ({
        key: decodeRow(entry, "key_json_data"),
        row: decodeRow(entry, "row_data"),
      }))
      .filter((entry) => matchesPrefix(entry.key, prefix))
      .sort((left, right) => compareRows(left.key, right.key));
    let emitted = 0;
    for (const entry of entries) {
      this.ensureActive();
      if (opts.after !== undefined && compareRows(entry.key, opts.after) <= 0) {
        continue;
      }
      if (opts.limit !== undefined && emitted >= opts.limit) {
        break;
      }
      emitted += 1;
      yield entry.row;
    }
  }

  async compareAndApply(table: string, key: Row, expect: unknown, next: unknown): Promise<boolean> {
    this.ensureActive();
    const row =
      typeof next === "object" && next !== null && !Array.isArray(next)
        ? (next as Row)
        : { value: next };
    const keyJson = keyOf(key);
    // An empty value encodes undefined, which JSON cannot represent.
    const valueJson = next === undefined ? "" : canonicalize(next);
    if (expect === undefined) {
      return (
        this.connection.run(
          `INSERT INTO ${this.table} (table_name, key_json, key_json_data, row_data, value_data)
           VALUES (?, ?, ?, ?, ?) ON CONFLICT (table_name, key_json) DO NOTHING`,
          table,
          keyJson,
          keyJson,
          canonicalize(row),
          valueJson,
        ) === 1
      );
    }
    return (
      this.connection.run(
        `UPDATE ${this.table} SET row_data = ?, value_data = ?
         WHERE table_name = ? AND key_json = ? AND value_data = ?`,
        canonicalize(row),
        valueJson,
        table,
        keyJson,
        canonicalize(expect),
      ) === 1
    );
  }

  private ensureActive(): void {
    if (!this.active) {
      throw new Error("Transaction is finished");
    }
  }
}

function decodeRow(result: unknown, column: string): Row {
  if (typeof result !== "object" || result === null || !(column in result)) {
    throw new Error(`Missing SQLite column: ${column}`);
  }
  const value = (result as Record<string, unknown>)[column];
  if (typeof value !== "string") {
    throw new Error(`Invalid SQLite JSON column: ${column}`);
  }
  return JSON.parse(value) as Row;
}

function isBun(): boolean {
  return "Bun" in globalThis;
}
