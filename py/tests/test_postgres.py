from __future__ import annotations

import asyncio
import os
import uuid
from typing import cast

import pytest
from psycopg import AsyncConnection, sql
from psycopg_pool import AsyncConnectionPool

from dockbay import PostgresStoreDriverOptions, create_postgres_driver

if not os.environ.get("DOCKBAY_TEST_POSTGRES_URL"):
    pytest.skip(
        "set DOCKBAY_TEST_POSTGRES_URL to run the Postgres driver suite",
        allow_module_level=True,
    )

POSTGRES_URL = cast(str, os.environ.get("DOCKBAY_TEST_POSTGRES_URL"))


def test_postgres_driver_upserts_scans_and_admits_one_cas_winner() -> None:
    table = f"store_driver_test_{uuid.uuid4().hex}"

    async def scenario() -> None:
        pool = AsyncConnectionPool(POSTGRES_URL, open=False)
        await pool.open()
        driver = create_postgres_driver(pool, PostgresStoreDriverOptions(table=table))
        try:
            await driver.transaction(
                lambda txn: txn.upsert("events", {"runId": "run_1", "seq": 2}, {"seq": 2})
            )
            await driver.transaction(
                lambda txn: txn.upsert("events", {"runId": "run_1", "seq": 1}, {"seq": 1})
            )

            async def scan() -> list[dict[str, object]]:
                rows: list[dict[str, object]] = []

                async def work(txn):
                    async for row in txn.scan("events", {"runId": "run_1"}):
                        rows.append(row)

                await driver.transaction(work)
                return rows

            assert await scan() == [{"seq": 1}, {"seq": 2}]
            await driver.transaction(
                lambda txn: txn.compare_and_apply(
                    "locks", {"id": "budget"}, None, {"owner": "seed"}
                )
            )
            results = await asyncio.gather(
                *(
                    driver.transaction(
                        lambda txn, index=index: txn.compare_and_apply(
                            "locks", {"id": "budget"}, {"owner": "seed"}, {"owner": index}
                        )
                    )
                    for index in range(10)
                )
            )
            assert len([result for result in results if result]) == 1
        finally:
            async with await AsyncConnection.connect(POSTGRES_URL) as conn:
                await conn.execute(
                    sql.SQL("DROP TABLE IF EXISTS {table}").format(table=sql.Identifier(table))
                )
                await conn.commit()
            await driver.close()

    asyncio.run(scenario())
