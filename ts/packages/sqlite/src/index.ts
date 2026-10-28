import type { Row, ScanOptions, StoreDriver, Transaction } from "../../core/src/index.js";
import { canonicalize, compareRows, keyOf, matchesPrefix } from "../../core/src/index.js";
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
export interface SQLiteSyncTransaction {
  upsert(table: string, key: Row, row: Row): void;
  get(table: string, key: Row): Row | undefined;
  scan(table: string, prefix: Row, opts?: ScanOptions): Row[];
  compareAndApply(table: string, key: Row, expect: unknown, next: unknown): boolean;
  delete(table: string, key: Row): boolean;
  /** Read an existing physical table for migration; an absent table returns undefined. */
  readTable(table: string): Row[] | undefined;
}
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
  private pending = 0;
  private synchronous = false;

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

  /** Atomic synchronous work on a supplied connection, including within a caller's transaction. */
  transactionSync<T>(
    work: (transaction: SQLiteSyncTransaction) => T extends PromiseLike<unknown> ? never : T,
  ): T {
    if (this.closed) throw new Error("Store driver is closed");
    if (typeof this.source === "string")
      throw new Error("Synchronous transactions require a supplied SQLite connection");
    if (this.pending !== 0 || this.synchronous)
      throw new Error("Store driver transaction already queued or active");
    if (work.constructor.name === "AsyncFunction")
      throw new TypeError("Synchronous transactions cannot use asynchronous callbacks");
    this.synchronous = true;
    let connection: SQLiteConnection | undefined;
    let transaction: SQLiteRows | undefined;
    let started = false;
    const savepoint = `dockbay_${++savepointId}`;
    try {
      connection = this.ensureOpenSync();
      connection.exec(`SAVEPOINT ${savepoint}`);
      started = true;
      this.createTable(connection);
      transaction = new SQLiteRows(connection, this.table);
      const result = work(transaction);
      if (
        result !== null &&
        (typeof result === "object" || typeof result === "function") &&
        "then" in result &&
        typeof result.then === "function"
      ) {
        Promise.resolve(result).catch(() => {});
        throw new TypeError("Synchronous transactions cannot return a Promise");
      }
      connection.exec(`RELEASE SAVEPOINT ${savepoint}`);
      return result;
    } catch (error) {
      if (started && connection) {
        try {
          connection.exec(`ROLLBACK TO SAVEPOINT ${savepoint}`);
          connection.exec(`RELEASE SAVEPOINT ${savepoint}`);
        } catch {
          // Preserve the callback or SQLite error if SQLite already rolled back.
        }
      }
      throw error;
    } finally {
      transaction?.finish();
      this.synchronous = false;
    }
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
        this.createTable(connection);
        const txn = new SQLiteTransaction(connection, this.table);
        try {
          const result = await work(txn);
          connection.exec(savepoint === undefined ? "COMMIT" : `RELEASE SAVEPOINT ${savepoint}`);
          return result;
        } finally {
          txn.finish();
        }
      } catch (error) {
        try {
          if (savepoint === undefined) {
            connection.exec("ROLLBACK");
          } else {
            connection.exec(`ROLLBACK TO SAVEPOINT ${savepoint}`);
            connection.exec(`RELEASE SAVEPOINT ${savepoint}`);
          }
        } catch {
          // SQLite may have already aborted the transaction; preserve the original error.
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
    if (this.synchronous)
      return Promise.reject(new Error("Synchronous store driver transaction active"));
    this.pending += 1;
    const next = this.queue.then(work);
    this.queue = next.then(
      () => {
        this.pending -= 1;
      },
      () => {
        this.pending -= 1;
      },
    );
    return next;
  }

  private createTable(connection: SQLiteConnection): void {
    connection.exec(`CREATE TABLE IF NOT EXISTS ${this.table} (
      table_name TEXT NOT NULL,
      key_json TEXT NOT NULL,
      key_json_data TEXT NOT NULL,
      row_data TEXT NOT NULL,
      value_data TEXT NOT NULL,
      PRIMARY KEY (table_name, key_json)
    )`);
  }

  private ensureOpenSync(): SQLiteConnection {
    if (typeof this.source === "string")
      throw new Error("Synchronous transactions require a supplied SQLite connection");
    if (this.connection === undefined) {
      this.connection =
        "prepare" in this.source
          ? isBun()
            ? createBunSQLiteConnection(this.source)
            : createNodeSQLiteConnection(this.source)
          : this.source;
    }
    return this.connection;
  }

  private async ensureOpen(): Promise<SQLiteConnection> {
    if (this.connection !== undefined) {
      return this.connection;
    }
    if (typeof this.source !== "string") {
      return this.ensureOpenSync();
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

class SQLiteRows implements SQLiteSyncTransaction {
  private active = true;

  constructor(
    private readonly connection: SQLiteConnection,
    private readonly table: string,
  ) {}

  finish(): void {
    this.active = false;
  }

  upsert(table: string, key: Row, row: Row): void {
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

  get(table: string, key: Row): Row | undefined {
    this.ensureActive();
    const result = this.connection.get(
      `SELECT row_data FROM ${this.table} WHERE table_name = ? AND key_json = ?`,
      table,
      keyOf(key),
    );
    return result == null ? undefined : decodeRow(result, "row_data");
  }

  scan(table: string, prefix: Row, opts: ScanOptions = {}): Row[] {
    this.ensureActive();
    const entries = this.connection
      .all(`SELECT key_json_data, row_data FROM ${this.table} WHERE table_name = ?`, table)
      .map((entry) => ({
        key: decodeRow(entry, "key_json_data"),
        row: decodeRow(entry, "row_data"),
      }))
      .filter((entry) => matchesPrefix(entry.key, prefix))
      .sort((left, right) => compareRows(left.key, right.key));
    const rows: Row[] = [];
    for (const entry of entries) {
      this.ensureActive();
      if (opts.after !== undefined && compareRows(entry.key, opts.after) <= 0) {
        continue;
      }
      if (opts.limit !== undefined && rows.length >= opts.limit) {
        break;
      }
      rows.push(entry.row);
    }
    return rows;
  }

  compareAndApply(table: string, key: Row, expect: unknown, next: unknown): boolean {
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

  delete(table: string, key: Row): boolean {
    this.ensureActive();
    return (
      this.connection.run(
        `DELETE FROM ${this.table} WHERE table_name = ? AND key_json = ?`,
        table,
        keyOf(key),
      ) === 1
    );
  }

  readTable(table: string): Row[] | undefined {
    this.ensureActive();
    if (!IDENTIFIER.test(table)) throw new Error(`Invalid SQLite table: ${table}`);
    if (
      !this.connection.get(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
        table,
      )
    )
      return undefined;
    return this.connection.all(`SELECT * FROM "${table}"`) as Row[];
  }

  ensureActive(): void {
    if (!this.active) {
      throw new Error("Transaction is finished");
    }
  }
}

class SQLiteTransaction implements Transaction {
  private readonly rows: SQLiteRows;
  constructor(connection: SQLiteConnection, table: string) {
    this.rows = new SQLiteRows(connection, table);
  }
  finish(): void {
    this.rows.finish();
  }
  async upsert(table: string, key: Row, row: Row): Promise<void> {
    this.rows.upsert(table, key, row);
  }
  async get(table: string, key: Row): Promise<Row | undefined> {
    return this.rows.get(table, key);
  }
  async *scan(table: string, prefix: Row, opts: ScanOptions = {}): AsyncIterable<Row> {
    for (const row of this.rows.scan(table, prefix, opts)) {
      this.rows.ensureActive();
      yield row;
    }
  }
  async compareAndApply(table: string, key: Row, expect: unknown, next: unknown): Promise<boolean> {
    return this.rows.compareAndApply(table, key, expect, next);
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
