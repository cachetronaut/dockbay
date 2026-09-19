from __future__ import annotations

import asyncio
import sqlite3
import tempfile
from pathlib import Path

import pytest

from dockbay import ScanOptions, SQLiteStoreDriverOptions, create_sqlite_driver


def test_sqlite_driver_upserts_idempotently_and_gets_rows() -> None:
    asyncio.run(_assert_sqlite_driver_upserts_idempotently_and_gets_rows())


async def _assert_sqlite_driver_upserts_idempotently_and_gets_rows() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        driver = create_sqlite_driver(Path(tmp) / "test.db")
        try:

            async def work(txn) -> None:
                await txn.upsert("events", {"runId": "run_1", "seq": 1}, {"type": "stage"})
                await txn.upsert("events", {"runId": "run_1", "seq": 1}, {"type": "stage"})
                assert await txn.get("events", {"runId": "run_1", "seq": 1}) == {"type": "stage"}

            await driver.transaction(work)

            async def verify(txn) -> None:
                assert await txn.get("events", {"runId": "run_1", "seq": 1}) == {"type": "stage"}
                assert await txn.get("events", {"runId": "run_1", "seq": 99}) is None

            await driver.transaction(verify)
        finally:
            await driver.close()


def test_sqlite_driver_scans_rows_in_key_order_by_prefix() -> None:
    asyncio.run(_assert_sqlite_driver_scans_rows_in_key_order_by_prefix())


async def _assert_sqlite_driver_scans_rows_in_key_order_by_prefix() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        driver = create_sqlite_driver(Path(tmp) / "test.db")
        try:
            await driver.transaction(
                lambda txn: txn.upsert("events", {"runId": "run_1", "seq": 2}, {"seq": 2})
            )
            await driver.transaction(
                lambda txn: txn.upsert("events", {"runId": "run_2", "seq": 1}, {"seq": 1})
            )
            await driver.transaction(
                lambda txn: txn.upsert("events", {"runId": "run_1", "seq": 1}, {"seq": 1})
            )

            async def scan_prefix(txn) -> list[dict[str, object]]:
                rows: list[dict[str, object]] = []
                async for row in txn.scan("events", {"runId": "run_1"}):
                    rows.append(row)
                return rows

            rows = await driver.transaction(scan_prefix)
            assert rows == [{"seq": 1}, {"seq": 2}]
        finally:
            await driver.close()


def test_sqlite_driver_scans_with_after_and_limit() -> None:
    asyncio.run(_assert_sqlite_driver_scans_with_after_and_limit())


async def _assert_sqlite_driver_scans_with_after_and_limit() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        driver = create_sqlite_driver(Path(tmp) / "test.db")
        try:
            for seq in range(1, 6):
                await driver.transaction(
                    lambda txn, s=seq: txn.upsert(
                        "events", {"runId": "run_1", "seq": s}, {"seq": s}
                    )
                )

            async def scan_after(txn) -> list[dict[str, object]]:
                rows: list[dict[str, object]] = []
                async for row in txn.scan(
                    "events",
                    {"runId": "run_1"},
                    ScanOptions(after={"runId": "run_1", "seq": 2}, limit=2),
                ):
                    rows.append(row)
                return rows

            rows = await driver.transaction(scan_after)
            assert rows == [{"seq": 3}, {"seq": 4}]
        finally:
            await driver.close()


def test_sqlite_driver_compare_and_apply_admits_one_winner_under_contention() -> None:
    asyncio.run(_assert_sqlite_driver_compare_and_apply_admits_one_winner_under_contention())


async def _assert_sqlite_driver_compare_and_apply_admits_one_winner_under_contention() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        driver = create_sqlite_driver(Path(tmp) / "test.db")
        try:

            async def seed(txn) -> bool:
                return await txn.compare_and_apply(
                    "locks", {"id": "budget"}, None, {"owner": "seed"}
                )

            await driver.transaction(seed)

            async def attempt(index: int) -> bool:
                async def work(txn) -> bool:
                    return await txn.compare_and_apply(
                        "locks", {"id": "budget"}, {"owner": "seed"}, {"owner": index}
                    )

                return await driver.transaction(work)

            results = await asyncio.gather(*(attempt(index) for index in range(10)))

            assert len([result for result in results if result]) == 1
        finally:
            await driver.close()


def test_sqlite_driver_compare_and_apply_insert_only_if_absent() -> None:
    asyncio.run(_assert_sqlite_driver_compare_and_apply_insert_only_if_absent())


async def _assert_sqlite_driver_compare_and_apply_insert_only_if_absent() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        driver = create_sqlite_driver(Path(tmp) / "test.db")
        try:
            first = await driver.transaction(
                lambda txn: txn.compare_and_apply(
                    "locks", {"id": "singleton"}, None, {"owner": "first"}
                )
            )
            second = await driver.transaction(
                lambda txn: txn.compare_and_apply(
                    "locks", {"id": "singleton"}, None, {"owner": "second"}
                )
            )
            assert first is True
            assert second is False

            async def verify(txn) -> dict[str, object] | None:
                return await txn.get("locks", {"id": "singleton"})

            row = await driver.transaction(verify)
            assert row == {"owner": "first"}
        finally:
            await driver.close()


def test_sqlite_driver_compare_and_apply_with_reordered_keys() -> None:
    asyncio.run(_assert_sqlite_driver_compare_and_apply_with_reordered_keys())


async def _assert_sqlite_driver_compare_and_apply_with_reordered_keys() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        driver = create_sqlite_driver(Path(tmp) / "test.db")
        try:
            await driver.transaction(
                lambda txn: txn.compare_and_apply("state", {"id": "s1"}, None, {"b": 1, "a": 2})
            )

            applied = await driver.transaction(
                lambda txn: txn.compare_and_apply("state", {"id": "s1"}, {"a": 2, "b": 1}, {"c": 3})
            )
            assert applied is True

            async def verify(txn) -> dict[str, object] | None:
                return await txn.get("state", {"id": "s1"})

            row = await driver.transaction(verify)
            assert row == {"c": 3}
        finally:
            await driver.close()


def test_sqlite_driver_compare_and_apply_after_get_round_trip() -> None:
    asyncio.run(_assert_sqlite_driver_compare_and_apply_after_get_round_trip())


async def _assert_sqlite_driver_compare_and_apply_after_get_round_trip() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        driver = create_sqlite_driver(Path(tmp) / "test.db")
        try:
            await driver.transaction(
                lambda txn: txn.upsert("state", {"id": "s1"}, {"z": 1, "a": 2})
            )

            async def get_then_cas(txn) -> bool:
                current = await txn.get("state", {"id": "s1"})
                assert current is not None
                return await txn.compare_and_apply(
                    "state", {"id": "s1"}, current, {"updated": True}
                )

            applied = await driver.transaction(get_then_cas)
            assert applied is True
        finally:
            await driver.close()


def test_sqlite_driver_persists_across_reopen() -> None:
    asyncio.run(_assert_sqlite_driver_persists_across_reopen())


async def _assert_sqlite_driver_persists_across_reopen() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "test.db"

        driver = create_sqlite_driver(db_path)
        await driver.transaction(
            lambda txn: txn.upsert("events", {"id": "e1"}, {"type": "created"})
        )
        await driver.close()

        driver = create_sqlite_driver(db_path)
        try:

            async def verify(txn) -> dict[str, object] | None:
                return await txn.get("events", {"id": "e1"})

            row = await driver.transaction(verify)
            assert row == {"type": "created"}
        finally:
            await driver.close()


def test_sqlite_driver_custom_table_name() -> None:
    asyncio.run(_assert_sqlite_driver_custom_table_name())


async def _assert_sqlite_driver_custom_table_name() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "test.db"
        driver = create_sqlite_driver(db_path, SQLiteStoreDriverOptions(table="custom_rows"))
        try:
            await driver.transaction(
                lambda txn: txn.upsert("events", {"id": "e1"}, {"type": "test"})
            )

            async def verify(txn) -> dict[str, object] | None:
                return await txn.get("events", {"id": "e1"})

            row = await driver.transaction(verify)
            assert row == {"type": "test"}

            conn = sqlite3.connect(db_path)
            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='custom_rows'"
            )
            assert cursor.fetchone() is not None, "custom_rows table must exist in SQLite"
            conn.close()
        finally:
            await driver.close()


def test_sqlite_driver_rejects_invalid_table_name() -> None:
    with pytest.raises(ValueError, match="Invalid SQLite store-driver table"):
        create_sqlite_driver(":memory:", SQLiteStoreDriverOptions(table="bad;table"))


def test_sqlite_driver_rejects_reserved_word_table_name() -> None:
    with pytest.raises(ValueError, match="SQLite reserved word"):
        create_sqlite_driver(":memory:", SQLiteStoreDriverOptions(table="select"))

    with pytest.raises(ValueError, match="SQLite reserved word"):
        create_sqlite_driver(":memory:", SQLiteStoreDriverOptions(table="SELECT"))


def test_sqlite_driver_transaction_rolls_back_on_error() -> None:
    asyncio.run(_assert_sqlite_driver_transaction_rolls_back_on_error())


async def _assert_sqlite_driver_transaction_rolls_back_on_error() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        driver = create_sqlite_driver(Path(tmp) / "test.db")
        try:

            async def failing_work(txn) -> None:
                await txn.upsert("events", {"id": "e1"}, {"type": "should_not_persist"})
                raise RuntimeError("deliberate failure")

            with pytest.raises(RuntimeError, match="deliberate failure"):
                await driver.transaction(failing_work)

            async def verify(txn) -> dict[str, object] | None:
                return await txn.get("events", {"id": "e1"})

            row = await driver.transaction(verify)
            assert row is None
        finally:
            await driver.close()


def test_sqlite_driver_close_then_transaction_raises() -> None:
    asyncio.run(_assert_sqlite_driver_close_then_transaction_raises())


async def _assert_sqlite_driver_close_then_transaction_raises() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        driver = create_sqlite_driver(Path(tmp) / "test.db")
        await driver.transaction(lambda txn: txn.upsert("events", {"id": "e1"}, {"type": "test"}))
        await driver.close()

        with pytest.raises(RuntimeError, match="Store driver is closed"):
            await driver.transaction(lambda txn: txn.get("events", {"id": "e1"}))
