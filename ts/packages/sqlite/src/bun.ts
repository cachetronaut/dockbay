import type { SQLiteConnection, SQLiteDatabase, SQLiteStatement } from "./connection.js";

export function createBunSQLiteConnection(database: SQLiteDatabase): SQLiteConnection {
  const withStatement = <T>(sql: string, work: (statement: SQLiteStatement) => T): T => {
    const statement = database.prepare(sql);
    try {
      return work(statement);
    } finally {
      statement.finalize?.();
    }
  };
  return {
    exec: (sql) => database.exec(sql),
    run: (sql, ...parameters) =>
      withStatement(sql, (statement) => Number(statement.run(...parameters).changes)),
    get: (sql, ...parameters) =>
      withStatement(sql, (statement) => statement.get(...parameters) ?? undefined),
    all: (sql, ...parameters) => withStatement(sql, (statement) => statement.all(...parameters)),
    close: () => database.close(),
  };
}

export async function openBunSQLiteConnection(path: string): Promise<SQLiteConnection> {
  const moduleName = "bun:sqlite";
  const { Database } = (await import(moduleName)) as {
    Database: new (path: string) => SQLiteDatabase;
  };
  return createBunSQLiteConnection(new Database(path));
}
