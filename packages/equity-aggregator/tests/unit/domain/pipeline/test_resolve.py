# pipeline/test_resolve.py

import asyncio
from collections.abc import AsyncIterable

import pytest

from equity_aggregator.domain.pipeline.resolve import (
    FeedRecord,
    _consume,
    _produce,
    resolve,
)

pytestmark = pytest.mark.unit


async def _generate_values(*values: int) -> AsyncIterable[dict[str, int]]:
    for value in values:
        yield {"value": value}


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


async def test_resolve_yields_all_items() -> None:
    """
    ARRANGE: a feed generator yielding two items and a dummy model
    ACT:     call resolve with the feed and model
    ASSERT:  yields exactly two records
    """
    feeds = ((lambda: _generate_values(1, 2), DummyModel),)
    expected_records = 2

    records = [record async for record in resolve(feeds)]

    assert len(records) == expected_records


async def test_resolve_propagates_models() -> None:
    """
    ARRANGE: a feed generator yielding one item and a dummy model
    ACT:     call resolve with the feed and model
    ASSERT:  record.model is the dummy model
    """
    feeds = ((lambda: _generate_values(7), DummyModel),)

    records = [record async for record in resolve(feeds)]

    assert records[0].model is DummyModel


async def test_resolve_merges_multiple_feeds() -> None:
    """
    ARRANGE: two feed generators yielding different items and a dummy model
    ACT:     call resolve with both feeds and model
    ASSERT:  yields all items from both feeds
    """
    feeds = (
        (lambda: _generate_values(1), DummyModel),
        (lambda: _generate_values(2, 3), DummyModel),
    )

    records = [record async for record in resolve(feeds)]

    assert {record.raw_data["value"] for record in records} == {1, 2, 3}


async def test_resolve_ignores_failing_feed() -> None:
    """
    ARRANGE: one feed generator that raises an error and one that yields an item
    ACT:     call resolve with both feeds and model
    ASSERT:  yields only the item from the successful feed
    """

    async def _boom() -> AsyncIterable[dict[str, int]]:
        raise RuntimeError("fail")
        if False:
            yield {}

    feeds = (
        (_boom, DummyModel),
        (lambda: _generate_values(42), DummyModel),
    )

    records = [record async for record in resolve(feeds)]

    assert records[0].raw_data == {"value": 42}
