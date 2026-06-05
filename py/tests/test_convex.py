from __future__ import annotations

import asyncio

import pytest

from dockbay import (
    ConvexOperationKindError,
    ConvexStoreOperation,
    InMemoryConvexOperationHost,
    MissingConvexOperationError,
)


def test_dispatches_named_mutation_operation() -> None:
    asyncio.run(_assert_dispatches_named_mutation_operation())


async def _assert_dispatches_named_mutation_operation() -> None:
    async def run(ctx, input_value):
        assert ctx.kind == "mutation"
        assert isinstance(input_value, dict)
        return {"ok": True, "reserved": input_value["amount"]}

    driver = InMemoryConvexOperationHost(
        [ConvexStoreOperation(name="budget.tryReserve", kind="mutation", run=run)]
    ).create_driver()

    assert await driver.call("budget.tryReserve", {"amount": 2}) == {
        "ok": True,
        "reserved": 2,
    }


def test_dispatches_queries_separately_from_mutations() -> None:
    asyncio.run(_assert_dispatches_queries_separately_from_mutations())


async def _assert_dispatches_queries_separately_from_mutations() -> None:
    async def run(ctx, input_value):
        assert ctx.kind == "query"
        assert isinstance(input_value, dict)
        return {"revoked": input_value["jti"] == "jti_1"}

    host = InMemoryConvexOperationHost(
        [ConvexStoreOperation(name="revocation.isRevoked", kind="query", run=run)]
    )

    assert await host.call_query("revocation.isRevoked", {"jti": "jti_1"}) == {"revoked": True}
    with pytest.raises(ConvexOperationKindError):
        await host.call_mutation("revocation.isRevoked", {"jti": "jti_1"})


def test_fails_clearly_when_operation_is_missing() -> None:
    asyncio.run(_assert_fails_clearly_when_operation_is_missing())


async def _assert_fails_clearly_when_operation_is_missing() -> None:
    driver = InMemoryConvexOperationHost().create_driver()

    with pytest.raises(MissingConvexOperationError):
        await driver.call("budget.tryReserve", {"amount": 1})
