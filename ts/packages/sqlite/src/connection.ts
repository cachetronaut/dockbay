export interface SQLiteStatement {
  run(...parameters: string[]): { readonly changes: number | bigint };
  get(...parameters: string[]): unknown;
  all(...parameters: string[]): unknown[];
  finalize?(): void;
}

export interface SQLiteDatabase {
  exec(sql: string): unknown;
  prepare(sql: string): SQLiteStatement;
  close(): void;
}

export interface SQLiteConnection {
  exec(sql: string): void;
  run(sql: string, ...parameters: string[]): number;
  get(sql: string, ...parameters: string[]): unknown;
  all(sql: string, ...parameters: string[]): unknown[];
  close(): void;
}
