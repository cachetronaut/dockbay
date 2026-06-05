import type { Row, ScanOptions, StoreDriver, Transaction } from "@dockbay/core";
import { compareRows, keyOf } from "@dockbay/core";
import type { Pool, PoolClient, QueryResultRow } from "pg";

export interface PostgresStoreDriverOptions {
  readonly table?: string;
}

const DEFAULT_TABLE = "store_driver_rows";
const IDENTIFIER = /^[A-Za-z_][A-Za-z0-9_]*$/;

export class PostgresStoreDriver implements StoreDriver {
  readonly backend = "postgres";
  private readonly table: string;
  private ready = false;

  constructor(
    private readonly pool: Pool,
    options: PostgresStoreDriverOptions = {},
  ) {
    this.table = options.table ?? DEFAULT_TABLE;
    if (!IDENTIFIER.test(this.table)) {
      throw new Error(`Invalid Postgres store-driver table: ${this.table}`);
    }
  }

  async transaction<T>(work: (txn: Transaction) => Promise<T>): Promise<T> {
    await this.ensureReady();
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      const result = await work(new PostgresTransaction(client, this.table));
      await client.query("COMMIT");
      return result;
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }

  async close(): Promise<void> {
    await this.pool.end();
  }

  private async ensureReady(): Promise<void> {
    if (this.ready) {
      return;
    }
    await this.pool.query(
      `CREATE TABLE IF NOT EXISTS ${quoteIdent(this.table)} (
        table_name text NOT NULL,
        key_json text NOT NULL,
        key jsonb NOT NULL,
        row jsonb NOT NULL,
        value jsonb NOT NULL,
        PRIMARY KEY (table_name, key_json)
      )`,
    );
    this.ready = true;
  }
}

export function createPostgresDriver(
  pool: Pool,
  options: PostgresStoreDriverOptions = {},
): PostgresStoreDriver {
  return new PostgresStoreDriver(pool, options);
}

class PostgresTransaction implements Transaction {
  constructor(
    private readonly client: PoolClient,
    private readonly table: string,
  ) {}

  async upsert(table: string, key: Row, row: Row): Promise<void> {
    const keyJson = keyOf(key);
    await this.client.query(
      `INSERT INTO ${quoteIdent(this.table)} (table_name, key_json, key, row, value)
       VALUES ($1, $2, $3::jsonb, $4::jsonb, $4::jsonb)
       ON CONFLICT (table_name, key_json) DO UPDATE SET key = EXCLUDED.key, row = EXCLUDED.row`,
      [table, keyJson, JSON.stringify(key), JSON.stringify(row)],
    );
  }

  async get(table: string, key: Row): Promise<Row | undefined> {
    const result = await this.client.query(
      `SELECT row FROM ${quoteIdent(this.table)} WHERE table_name = $1 AND key_json = $2`,
      [table, keyOf(key)],
    );
    const first = result.rows[0] as { row: Row } | undefined;
    return first?.row;
  }

  async *scan(table: string, prefix: Row, opts: ScanOptions = {}): AsyncIterable<Row> {
    const result = await this.client.query(
      `SELECT key, row FROM ${quoteIdent(this.table)} WHERE table_name = $1`,
      [table],
    );
    const rows = result.rows
      .map((entry: QueryResultRow) => ({ key: entry.key as Row, row: entry.row as Row }))
      .filter((entry) => matchesPrefix(entry.key, prefix))
      .sort((left, right) => compareRows(left.key, right.key));
    let emitted = 0;
    for (const entry of rows) {
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
    const nextRow =
      typeof next === "object" && next !== null && !Array.isArray(next)
        ? (next as Row)
        : { value: next };
    const keyJson = keyOf(key);
    if (expect === undefined) {
      const result = await this.client.query(
        `INSERT INTO ${quoteIdent(this.table)} (table_name, key_json, key, row, value)
         VALUES ($1, $2, $3::jsonb, $4::jsonb, $4::jsonb)
         ON CONFLICT (table_name, key_json) DO NOTHING`,
        [table, keyJson, JSON.stringify(key), JSON.stringify(nextRow)],
      );
      return result.rowCount === 1;
    }
    const result = await this.client.query(
      `UPDATE ${quoteIdent(this.table)} SET row = $3::jsonb, value = $3::jsonb
       WHERE table_name = $1 AND key_json = $2 AND row = $4::jsonb`,
      [table, keyJson, JSON.stringify(nextRow), JSON.stringify(expect)],
    );
    return result.rowCount === 1;
  }
}

function matchesPrefix(key: Row, prefix: Row): boolean {
  for (const [name, expected] of Object.entries(prefix)) {
    if (keyOf({ value: key[name] }) !== keyOf({ value: expected })) {
      return false;
    }
  }
  return true;
}

function quoteIdent(value: string): string {
  return `"${value.replaceAll('"', '""')}"`;
}
