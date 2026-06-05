import type { Row, ScanOptions, StoreDriver, Transaction } from "@dockbay/core";
import { compareRows, keyOf, matchesPrefix } from "@dockbay/core";

type Table = Map<string, { key: Row; row: Row; value: unknown }>;

export class InMemoryStoreDriver implements StoreDriver {
  readonly backend = "memory";
  private readonly tables = new Map<string, Table>();
  private queue = Promise.resolve();
  private closed = false;

  async transaction<T>(work: (txn: Transaction) => Promise<T>): Promise<T> {
    if (this.closed) {
      throw new Error("Store driver is closed");
    }
    const next = this.queue.then(() => work(new InMemoryTransaction(this.tables)));
    this.queue = next.then(
      () => undefined,
      () => undefined,
    );
    return next;
  }

  async close(): Promise<void> {
    this.closed = true;
  }
}

export function createInMemoryDriver(): InMemoryStoreDriver {
  return new InMemoryStoreDriver();
}

class InMemoryTransaction implements Transaction {
  constructor(private readonly tables: Map<string, Table>) {}

  async upsert(table: string, key: Row, row: Row): Promise<void> {
    this.table(table).set(keyOf(key), { key, row, value: row });
  }

  async get(table: string, key: Row): Promise<Row | undefined> {
    return this.table(table).get(keyOf(key))?.row;
  }

  async *scan(table: string, prefix: Row, opts: ScanOptions = {}): AsyncIterable<Row> {
    const rows = [...this.table(table).values()]
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
    const rows = this.table(table);
    const id = keyOf(key);
    const current = rows.get(id);
    const currentValue = current?.value;
    if (keyOf({ value: currentValue }) !== keyOf({ value: expect })) {
      return false;
    }
    const row =
      typeof next === "object" && next !== null && !Array.isArray(next)
        ? (next as Row)
        : { value: next };
    rows.set(id, { key, row, value: next });
    return true;
  }

  private table(name: string): Table {
    const existing = this.tables.get(name);
    if (existing !== undefined) {
      return existing;
    }
    const table = new Map<string, { key: Row; row: Row; value: unknown }>();
    this.tables.set(name, table);
    return table;
  }
}
