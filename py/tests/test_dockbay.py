from __future__ import annotations

import asyncio

from dockbay import canonicalize, create_in_memory_driver, matches_prefix


def test_canonicalize_and_prefix_matching() -> None:
    assert canonicalize({"b": 2, "a": 1}) == '{"a":1,"b":2}'
    assert matches_prefix({"runId": "run_1", "seq": 2}, {"runId": "run_1"})
    assert not matches_prefix({"runId": "run_2", "seq": 2}, {"runId": "run_1"})


def test_upserts_idempotently_and_gets_rows() -> None:
    asyncio.run(_assert_upserts_idempotently_and_gets_rows())


async def _assert_upserts_idempotently_and_gets_rows() -> None:
    driver = create_in_memory_driver()

    async def work(txn) -> None:
        await txn.upsert("events", {"runId": "run_1", "seq": 1}, {"type": "stage"})
        await txn.upsert("events", {"runId": "run_1", "seq": 1}, {"type": "stage"})
        assert await txn.get("events", {"runId": "run_1", "seq": 1}) == {"type": "stage"}

    await driver.transaction(work)


def test_scans_rows_in_key_order_by_prefix() -> None:
    asyncio.run(_assert_scans_rows_in_key_order_by_prefix())


async def _assert_scans_rows_in_key_order_by_prefix() -> None:
    driver = create_in_memory_driver()

    async def work(txn) -> None:
        await txn.upsert("events", {"runId": "run_1", "seq": 2}, {"seq": 2})
        await txn.upsert("events", {"runId": "run_2", "seq": 1}, {"seq": 1})
        await txn.upsert("events", {"runId": "run_1", "seq": 1}, {"seq": 1})
        rows = []
        async for row in txn.scan("events", {"runId": "run_1"}):
            rows.append(row)
        assert rows == [{"seq": 1}, {"seq": 2}]

    await driver.transaction(work)


def test_compare_and_apply_admits_one_winner_under_contention() -> None:
    asyncio.run(_assert_compare_and_apply_admits_one_winner_under_contention())


async def _assert_compare_and_apply_admits_one_winner_under_contention() -> None:
    driver = create_in_memory_driver()

    async def seed(txn) -> bool:
        return await txn.compare_and_apply("locks", {"id": "budget"}, None, {"owner": "seed"})

    await driver.transaction(seed)

    async def attempt(index: int) -> bool:
        async def work(txn) -> bool:
            return await txn.compare_and_apply(
                "locks", {"id": "budget"}, {"owner": "seed"}, {"owner": index}
            )

        return await driver.transaction(work)

    results = await asyncio.gather(*(attempt(index) for index in range(10)))

    assert len([result for result in results if result]) == 1
