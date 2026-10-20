import { createRequire } from "node:module";
import type { SQLiteConnection, SQLiteDatabase } from "./connection.js";

export function createNodeSQLiteConnection(database: SQLiteDatabase): SQLiteConnection {
  return {
    exec: (sql) => database.exec(sql),
    run: (sql, ...parameters) => Number(database.prepare(sql).run(...parameters).changes),
    get: (sql, ...parameters) => database.prepare(sql).get(...parameters),
    all: (sql, ...parameters) => database.prepare(sql).all(...parameters),
    close: () => database.close(),
  };
}

export async function openNodeSQLiteConnection(path: string): Promise<SQLiteConnection> {
  const { DatabaseSync } = createRequire(import.meta.url)(
    "node:sqlite",
  ) as typeof import("node:sqlite");
  return createNodeSQLiteConnection(new DatabaseSync(path));
}
