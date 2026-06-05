from __future__ import annotations

import asyncio
import json
import re
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Literal, Protocol, TypeAlias

from psycopg import AsyncConnection, sql
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool

Row = dict[str, Any]
JsonValue: TypeAlias = None | bool | int | float | str | list["JsonValue"] | dict[str, "JsonValue"]
ConvexOperationKind: TypeAlias = Literal["mutation", "query"]


@dataclass(frozen=True)
class ScanOptions:
    after: Row | None = None
    limit: int | None = None


@dataclass(frozen=True)
class MigrationSet:
    backend: str
    statements: list[str]


class Transaction(Protocol):
    async def upsert(self, table: str, key: Row, row: Row) -> None: ...
    async def get(self, table: str, key: Row) -> Row | None: ...
    def scan(
        self, table: str, prefix: Row, opts: ScanOptions | None = None
    ) -> AsyncIterator[Row]: ...
    async def compare_and_apply(
        self, table: str, key: Row, expect: Any, next_value: Any
    ) -> bool: ...


class StoreDriver(Protocol):
    backend: str

    async def transaction(self, work: Callable[[Transaction], Awaitable[Any]]) -> Any: ...
    async def close(self) -> None: ...


class ConvexOperationDriver(Protocol):
    async def call(self, operation: str, input_value: JsonValue) -> JsonValue: ...


@dataclass(frozen=True)
class ConvexOperationContext:
    kind: ConvexOperationKind


@dataclass(frozen=True)
class ConvexStoreOperation:
    name: str
    kind: ConvexOperationKind
    run: Callable[[ConvexOperationContext, JsonValue], Awaitable[JsonValue]]


class MissingConvexOperationError(Exception):
    def __init__(self, operation: str) -> None:
        super().__init__(f"Unknown Convex store operation: {operation}")
        self.operation = operation


class ConvexOperationKindError(Exception):
    def __init__(
        self, operation: str, expected: ConvexOperationKind, actual: ConvexOperationKind
    ) -> None:
        super().__init__(f"Convex store operation {operation} is {actual}; expected {expected}")
        self.operation = operation
        self.expected = expected
        self.actual = actual


class InMemoryConvexOperationHost:
    def __init__(self, operations: list[ConvexStoreOperation] | None = None) -> None:
        self._operations: dict[str, ConvexStoreOperation] = {}
        for operation in operations or []:
            self.register(operation)

    def register(self, operation: ConvexStoreOperation) -> None:
        if operation.name in self._operations:
            raise ValueError(f"Duplicate Convex store operation: {operation.name}")
        self._operations[operation.name] = operation

    async def call(self, operation: str, input_value: JsonValue) -> JsonValue:
        handler = self._operation(operation)
        return await handler.run(ConvexOperationContext(kind=handler.kind), input_value)

    async def call_mutation(self, operation: str, input_value: JsonValue) -> JsonValue:
        return await self._call_kind(operation, input_value, "mutation")

    async def call_query(self, operation: str, input_value: JsonValue) -> JsonValue:
        return await self._call_kind(operation, input_value, "query")

    def create_driver(self) -> ConvexOperationDriver:
        return _HostedConvexOperationDriver(self)

    async def _call_kind(
        self, operation: str, input_value: JsonValue, expected: ConvexOperationKind
    ) -> JsonValue:
        handler = self._operation(operation)
        if handler.kind != expected:
            raise ConvexOperationKindError(operation, expected, handler.kind)
        return await self.call(operation, input_value)

    def _operation(self, operation: str) -> ConvexStoreOperation:
        handler = self._operations.get(operation)
        if handler is None:
            raise MissingConvexOperationError(operation)
        return handler


class _HostedConvexOperationDriver:
    def __init__(self, host: InMemoryConvexOperationHost) -> None:
        self._host = host

    async def call(self, operation: str, input_value: JsonValue) -> JsonValue:
        return await self._host.call(operation, input_value)


def canonicalize(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=_json_default)


def key_of(key: Row) -> str:
    return canonicalize(key)


def matches_prefix(key: Row, prefix: Row) -> bool:
    for name, expected in prefix.items():
        if canonicalize(key.get(name)) != canonicalize(expected):
            return False
    return True


def compare_rows(left: Row, right: Row) -> int:
    left_key = key_of(left)
    right_key = key_of(right)
    if left_key < right_key:
        return -1
    if left_key > right_key:
        return 1
    return 0


class InMemoryStoreDriver:
    backend = "memory"

    def __init__(self) -> None:
        self._tables: dict[str, dict[str, _Entry]] = {}
        self._lock = asyncio.Lock()
        self._closed = False

    async def transaction(self, work: Callable[[Transaction], Awaitable[Any]]) -> Any:
        if self._closed:
            raise RuntimeError("Store driver is closed")
        async with self._lock:
            return await work(_InMemoryTransaction(self._tables))

    async def close(self) -> None:
        self._closed = True


def create_in_memory_driver() -> InMemoryStoreDriver:
    return InMemoryStoreDriver()


@dataclass(frozen=True)
class PostgresStoreDriverOptions:
    table: str = "store_driver_rows"


class PostgresStoreDriver:
    backend = "postgres"

    def __init__(
        self, pool: AsyncConnectionPool, options: PostgresStoreDriverOptions | None = None
    ):
        self._pool = pool
        self._table = (options or PostgresStoreDriverOptions()).table
        if not _IDENTIFIER.match(self._table):
            raise ValueError(f"Invalid Postgres store-driver table: {self._table}")
        self._ready = False

    async def transaction(self, work: Callable[[Transaction], Awaitable[Any]]) -> Any:
        await self._ensure_ready()
        async with self._pool.connection() as conn, conn.transaction():
            return await work(_PostgresTransaction(conn, self._table_sql()))

    async def close(self) -> None:
        await self._pool.close()

    def _table_sql(self) -> sql.Identifier:
        return sql.Identifier(self._table)

    async def _ensure_ready(self) -> None:
        if self._ready:
            return
        async with self._pool.connection() as conn:
            await conn.execute(
                sql.SQL(
                    "CREATE TABLE IF NOT EXISTS {table} ("
                    " table_name text NOT NULL,"
                    " key_json text NOT NULL,"
                    " key jsonb NOT NULL,"
                    " row jsonb NOT NULL,"
                    " value jsonb NOT NULL,"
                    " PRIMARY KEY (table_name, key_json)"
                    ")"
                ).format(table=self._table_sql())
            )
        self._ready = True


def create_postgres_driver(
    pool: AsyncConnectionPool, options: PostgresStoreDriverOptions | None = None
) -> PostgresStoreDriver:
    return PostgresStoreDriver(pool, options)


@dataclass(frozen=True)
class _Entry:
    key: Row
    row: Row
    value: Any


class _InMemoryTransaction:
    def __init__(self, tables: dict[str, dict[str, _Entry]]) -> None:
        self._tables = tables

    async def upsert(self, table: str, key: Row, row: Row) -> None:
        self._table(table)[key_of(key)] = _Entry(key=key, row=row, value=row)

    async def get(self, table: str, key: Row) -> Row | None:
        entry = self._table(table).get(key_of(key))
        return entry.row if entry is not None else None

    async def scan(
        self, table: str, prefix: Row, opts: ScanOptions | None = None
    ) -> AsyncIterator[Row]:
        options = opts or ScanOptions()
        rows = sorted(
            [entry for entry in self._table(table).values() if matches_prefix(entry.key, prefix)],
            key=lambda entry: key_of(entry.key),
        )
        emitted = 0
        for entry in rows:
            if options.after is not None and compare_rows(entry.key, options.after) <= 0:
                continue
            if options.limit is not None and emitted >= options.limit:
                break
            emitted += 1
            yield entry.row

    async def compare_and_apply(self, table: str, key: Row, expect: Any, next_value: Any) -> bool:
        rows = self._table(table)
        entry_id = key_of(key)
        current = rows.get(entry_id)
        current_value = current.value if current is not None else None
        if key_of({"value": current_value}) != key_of({"value": expect}):
            return False
        row = next_value if isinstance(next_value, dict) else {"value": next_value}
        rows[entry_id] = _Entry(key=key, row=row, value=next_value)
        return True

    def _table(self, name: str) -> dict[str, _Entry]:
        if name not in self._tables:
            self._tables[name] = {}
        return self._tables[name]


class _PostgresTransaction:
    def __init__(self, conn: AsyncConnection[Any], table_sql: sql.Identifier) -> None:
        self._conn = conn
        self._table_sql = table_sql

    async def upsert(self, table: str, key: Row, row: Row) -> None:
        key_json = key_of(key)
        statement = sql.SQL(
            "INSERT INTO {table} (table_name, key_json, key, row, value) "
            "VALUES (%s, %s, %s, %s, %s) "
            "ON CONFLICT (table_name, key_json) DO UPDATE SET "
            "key = EXCLUDED.key, row = EXCLUDED.row"
        ).format(table=self._table_sql)
        await self._conn.execute(statement, (table, key_json, Jsonb(key), Jsonb(row), Jsonb(row)))

    async def get(self, table: str, key: Row) -> Row | None:
        statement = sql.SQL(
            "SELECT row FROM {table} WHERE table_name = %s AND key_json = %s"
        ).format(table=self._table_sql)
        cursor = await self._conn.execute(statement, (table, key_of(key)))
        result = await cursor.fetchone()
        return None if result is None else result[0]

    async def scan(
        self, table: str, prefix: Row, opts: ScanOptions | None = None
    ) -> AsyncIterator[Row]:
        options = opts or ScanOptions()
        statement = sql.SQL("SELECT key, row FROM {table} WHERE table_name = %s").format(
            table=self._table_sql
        )
        cursor = await self._conn.execute(statement, (table,))
        rows = sorted(
            [
                {"key": key, "row": row}
                for key, row in await cursor.fetchall()
                if matches_prefix(key, prefix)
            ],
            key=lambda entry: key_of(entry["key"]),
        )
        emitted = 0
        for entry in rows:
            if options.after is not None and compare_rows(entry["key"], options.after) <= 0:
                continue
            if options.limit is not None and emitted >= options.limit:
                break
            emitted += 1
            yield entry["row"]

    async def compare_and_apply(self, table: str, key: Row, expect: Any, next_value: Any) -> bool:
        row = next_value if isinstance(next_value, dict) else {"value": next_value}
        key_json = key_of(key)
        if expect is None:
            statement = sql.SQL(
                "INSERT INTO {table} (table_name, key_json, key, row, value) "
                "VALUES (%s, %s, %s, %s, %s) ON CONFLICT (table_name, key_json) DO NOTHING"
            ).format(table=self._table_sql)
            cursor = await self._conn.execute(
                statement, (table, key_json, Jsonb(key), Jsonb(row), Jsonb(row))
            )
            return cursor.rowcount == 1
        statement = sql.SQL(
            "UPDATE {table} SET row = %s, value = %s "
            "WHERE table_name = %s AND key_json = %s AND row = %s"
        ).format(table=self._table_sql)
        cursor = await self._conn.execute(
            statement, (Jsonb(row), Jsonb(row), table, key_json, Jsonb(expect))
        )
        return cursor.rowcount == 1


def _json_default(value: object) -> object:
    if hasattr(value, "__dict__"):
        return {key: item for key, item in vars(value).items() if item is not None}
    raise TypeError(f"Cannot serialize {type(value)!r}")


_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
