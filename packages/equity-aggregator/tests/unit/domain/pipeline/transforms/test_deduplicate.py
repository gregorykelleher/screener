# transforms/test_deduplicate.py

from collections.abc import AsyncIterator
from typing import Any

import pytest

from equity_aggregator.domain.pipeline.transforms.deduplicate import deduplicate
from equity_aggregator.schemas import RawEquity

pytestmark = pytest.mark.unit


async def _convert_to_async_iterable(records: list[Any]) -> AsyncIterator[Any]:
    """
    Convert a list of records into an asynchronous iterable.

    Args:
        records (list): The list of records to yield asynchronously.

    Yields:
        Any: Each record from the input list, yielded asynchronously.
    """
    for record in records:
        yield record


async def test_dedup_empty_list_returns_empty() -> None:
    """
    ARRANGE: no raw equities
    ACT:     deduplicate
    ASSERT:  returns empty list
    """
    actual = [equity async for equity in deduplicate(_convert_to_async_iterable([]))]

    assert actual == []


async def test_single_equity_round_trips() -> None:
    """
    ARRANGE: one equity, one share_class_figi
    ACT:     deduplicate
    ASSERT:  same object returned
    """
    raw_equity = RawEquity(
        name="SOLO CORP",
        symbol="SOLO",
        share_class_figi="FIGI00000001",
    )

    actual = [
        equity async for equity in deduplicate(_convert_to_async_iterable([raw_equity]))
    ]

    assert actual == [raw_equity]


async def test_grouping_by_share_class_figi() -> None:
    """
    ARRANGE: three records, two FIGI groups
    ACT:     deduplicate
    ASSERT:  yields one merged record per FIGI, in first-seen FIGI order
    """
    first_equity = RawEquity(name="A", symbol="A", share_class_figi="FIGI00000001")
    second_equity = RawEquity(name="B", symbol="B", share_class_figi="FIGI00000002")
    third_equity = RawEquity(name="C", symbol="C", share_class_figi="FIGI00000001")

    actual = [
        equity
        async for equity in deduplicate(
            _convert_to_async_iterable([first_equity, second_equity, third_equity]),
        )
    ]

    assert [record.share_class_figi for record in actual] == [
        "FIGI00000001",
        "FIGI00000002",
    ]
