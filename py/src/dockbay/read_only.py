"""SQLite reads with a coherent snapshot and no database initialization."""

from __future__ import annotations

import sqlite3
from collections.abc import AsyncIterator, Awaitable, Callable
from pathlib import Path
from typing import Any

from .core import Row, ScanOptions, SQLiteStoreDriver, Transaction


class ReadOnlyStoreUnavailable(RuntimeError):
    """A read-only entry point requires an existing DockBay database."""


class ReadOnlyTransaction:
    def __init__(self, transaction: Transaction) -> None:
        self._transaction = transaction

    async def get(self, table: str, key: Row) -> Row | None:
        return await self._transaction.get(table, key)

    def scan(self, table: str, prefix: Row, opts: ScanOptions | None = None) -> AsyncIterator[Row]:
        return self._transaction.scan(table, prefix, opts)

    async def upsert(self, table: str, key: Row, row: Row) -> None:
        raise PermissionError("this store is read-only")

    async def compare_and_apply(self, table: str, key: Row, expect: Any, next_value: Any) -> bool:
        raise PermissionError("this store is read-only")


class ReadOnlySQLiteStoreDriver(SQLiteStoreDriver):
    def _ensure_open(self) -> sqlite3.Connection:
        if self._conn is None:
            database = Path(self._path).resolve()
            if not database.is_file() or database.stat().st_size == 0:
                raise ReadOnlyStoreUnavailable("read-only access requires an existing database")
            connection = sqlite3.connect(
                database.as_uri() + "?mode=ro", uri=True, isolation_level="DEFERRED"
            )
            try:
                connection.execute("PRAGMA query_only=ON")
                table = connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                    (self._table,),
                ).fetchone()
                if table is None:
                    raise ReadOnlyStoreUnavailable("read-only access requires a DockBay database")
            except BaseException:
                connection.close()
                raise
            self._conn = connection
        return self._conn

    async def transaction(self, work: Callable[[Transaction], Awaitable[Any]]) -> Any:
        async def read(transaction: Transaction) -> Any:
            assert self._conn is not None
            self._conn.execute("BEGIN")
            return await work(ReadOnlyTransaction(transaction))

        return await super().transaction(read)
