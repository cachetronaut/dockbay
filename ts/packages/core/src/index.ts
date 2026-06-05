export type Row = Readonly<Record<string, unknown>>;

export interface ScanOptions {
  readonly after?: Row;
  readonly limit?: number;
}

export interface Transaction {
  upsert(table: string, key: Row, row: Row): Promise<void>;
  get(table: string, key: Row): Promise<Row | undefined>;
  scan(table: string, prefix: Row, opts?: ScanOptions): AsyncIterable<Row>;
  compareAndApply(table: string, key: Row, expect: unknown, next: unknown): Promise<boolean>;
}

export interface StoreDriver {
  readonly backend: string;
  transaction<T>(work: (txn: Transaction) => Promise<T>): Promise<T>;
  close(): Promise<void>;
}

export interface MigrationSet {
  readonly backend: string;
  readonly statements: readonly string[];
}

export function canonicalize(value: unknown): string {
  return JSON.stringify(value, replacer, 0);
}

export function keyOf(key: Row): string {
  return canonicalize(key);
}

export function matchesPrefix(key: Row, prefix: Row): boolean {
  for (const [name, expected] of Object.entries(prefix)) {
    if (canonicalize(key[name]) !== canonicalize(expected)) {
      return false;
    }
  }
  return true;
}

export function compareRows(left: Row, right: Row): number {
  return keyOf(left).localeCompare(keyOf(right));
}

function replacer(_key: string, value: unknown): unknown {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    return value;
  }
  return Object.fromEntries(
    Object.entries(value as Record<string, unknown>)
      .filter(([, entry]) => entry !== undefined)
      .sort(([left], [right]) => left.localeCompare(right)),
  );
}
