# pipeline/test_resolve.py

import asyncio
from collections.abc import AsyncIterable

import pytest

from equity_aggregator.domain.pipeline.resolve import (
    FeedRecord,
    _consume,
    _produce,
)

pytestmark = pytest.mark.unit


class DummyModel:
    """
    A dummy model class used as a placeholder in unit tests.

    This class does not implement any functionality and serves as a stand-in
    during testing.

    Args:
        None

    Returns:
        None
    """

    pass


async def dummy_fetch_success() -> AsyncIterable[dict[str, int]]:
    """
    Asynchronously yields two sample dictionary records for testing purposes.

    Yields:
        dict[str, int]: A dictionary containing a single key-value pair, where the key
            is a string and the value is an integer. Yields two records: {"alpha": 1}
            and {"beta": 2}.
    """
    yield {"alpha": 1}
    yield {"beta": 2}


async def dummy_fetch_empty() -> AsyncIterable[dict[str, int]]:
    """
    An asynchronous generator that yields no items.

    This function serves as a dummy async generator for testing purposes. It does not
    yield any values.

    Args:
        None

    Returns:
        AsyncIterable[dict[str, int]]: An asynchronous iterable that yields no items.
    """
    if False:
        yield {}


async def dummy_fetch_error() -> AsyncIterable[dict[str, int]]:
    """
    Simulates a fetch operation that always fails by raising a RuntimeError.

    Args:
        None

    Returns:
        AsyncIterable[dict[str, int]]: This function does not yield any values as it
        always raises an exception.

    Raises:
        RuntimeError: Always raised to simulate a fetch failure.
    """
    raise RuntimeError("fetch failure")
    if False:
        yield


async def test_produce_and_consume_success_returns_two_records() -> None:
    """
    ARRANGE: a queue and a fetcher yielding two records
    ACT:     call _produce then _consume
    ASSERT:  yields exactly two FeedRecord items
    """
    queue: asyncio.Queue[FeedRecord | None] = asyncio.Queue()
    expected_record_count = 2

    await _produce(dummy_fetch_success, DummyModel, queue)

    records = [record async for record in _consume(queue, total_producers=1)]

    assert len(records) == expected_record_count


async def test_produce_and_consume_items_are_feedrecord_instances() -> None:
    """
    ARRANGE: a queue and a fetcher yielding two records
    ACT:     call _produce then _consume
    ASSERT:  each yielded item is a FeedRecord
    """
    queue: asyncio.Queue[FeedRecord | None] = asyncio.Queue()

    await _produce(dummy_fetch_success, DummyModel, queue)

    records = [record async for record in _consume(queue, total_producers=1)]

    assert all(isinstance(record, FeedRecord) for record in records)


async def test_produce_and_consume_content_and_model_correct() -> None:
    """
    ARRANGE: a queue and a fetcher yielding two records
    ACT:     call _produce then _consume
    ASSERT:  record.model and raw_data match expected
    """
    queue: asyncio.Queue[FeedRecord | None] = asyncio.Queue()

    await _produce(dummy_fetch_success, DummyModel, queue)

    records = [record async for record in _consume(queue, total_producers=1)]

    expected = [
        (DummyModel, {"alpha": 1}),
        (DummyModel, {"beta": 2}),
    ]

    assert [(record.model, record.raw_data) for record in records] == expected


async def test_produce_and_consume_empty_feed_produces_no_records() -> None:
    """
    ARRANGE: a queue and a fetcher that yields nothing
    ACT:     call _produce then _consume
    ASSERT:  yields an empty list
    """
    queue: asyncio.Queue[FeedRecord | None] = asyncio.Queue()

    await _produce(dummy_fetch_empty, DummyModel, queue)

    records = [record async for record in _consume(queue, total_producers=1)]

    assert records == []


async def test_produce_and_consume_error_signals_completion_without_records() -> None:
    """
    ARRANGE: a queue and a fetcher that raises an error
    ACT:     call _produce then _consume
    ASSERT:  yields no items but still signals completion
    """
    queue: asyncio.Queue[FeedRecord | None] = asyncio.Queue()

    await _produce(dummy_fetch_error, DummyModel, queue)

    records = [record async for record in _consume(queue, total_producers=1)]

    assert records == []
