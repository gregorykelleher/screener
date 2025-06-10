# pipeline/test_resolve.py

import asyncio

import pytest

from equity_aggregator.domain.pipeline import (
    resolve,
)
from equity_aggregator.domain.pipeline.resolve import _safe_fetch

pytestmark = pytest.mark.unit


class DummyModel:
    """
    A placeholder model class used for testing the FeedRecord.model attribute.

    Args:
        None

    Returns:
        None
    """

    pass


async def dummy_fetch_success() -> list[dict[str, int]]:
    """
    Asynchronously fetches a list of dictionaries with string keys and integer values.

    Returns:
        list[dict[str, int]]: A list where each element is a dictionary mapping
            strings to integers.
    """
    return [{"alpha": 1}, {"beta": 2}]


async def dummy_fetch_empty() -> list[dict[str, int]]:
    """
    Asynchronously fetches an empty list of dictionaries.

    Args:
        None

    Returns:
        list[dict[str, int]]: An empty list of dictionaries with string keys and
            integer values.
    """
    return []


async def dummy_fetch_error() -> list[dict[str, int]]:
    """
    Asynchronously simulates a data fetch operation that always fails with RuntimeError.

    Raises:
        RuntimeError: Always raised to simulate a fetch failure.

    Returns:
        list[dict[str, int]]: This function never returns normally due to raised error.
    """
    raise RuntimeError("fetch failure")


async def test_safe_fetch_success_returns_data() -> None:
    """
    ARRANGE: fetcher returns data
    ACT:     call _safe_fetch
    ASSERT:  returns the same data
    """
    actual = await _safe_fetch(dummy_fetch_success, "Dummy", wait_timeout=1.0)

    assert actual == [{"alpha": 1}, {"beta": 2}]


async def test_safe_fetch_exception_returns_none() -> None:
    """
    ARRANGE: fetcher raises an exception
    ACT:     call _safe_fetch
    ASSERT:  returns None
    """
    actual = await _safe_fetch(dummy_fetch_error, "Dummy", wait_timeout=1.0)

    assert actual is None


async def test_safe_fetch_timeout_returns_none() -> None:
    """
    ARRANGE: fetcher sleeps beyond the timeout
    ACT:     call _safe_fetch with a short wait_timeout
    ASSERT:  returns None due to TimeoutError
    """

    async def long_fetch() -> list[dict[str, str]]:
        await asyncio.sleep(0.05)
        return [{"x": "y"}]

    actual = await _safe_fetch(long_fetch, "Dummy", wait_timeout=0.01)

    assert actual is None


async def test_resolve_feed_with_data_returns_two_records() -> None:
    """
    ARRANGE: feed_pair with successful fetcher and model
    ACT:     call _resolve_feed
    ASSERT:  returns two records
    """
    feed_pair = (dummy_fetch_success, DummyModel)
    expected_record_count = 2

    actual = await resolve._resolve_feed(feed_pair)

    assert len(actual) == expected_record_count


async def test_resolve_feed_records_are_feedrecord_instances() -> None:
    """
    ARRANGE: feed_pair with successful fetcher and model
    ACT:     call _resolve_feed
    ASSERT:  each record is a FeedRecord instance
    """
    feed_pair = (dummy_fetch_success, DummyModel)

    actual = await resolve._resolve_feed(feed_pair)

    assert all(isinstance(record, resolve.FeedRecord) for record in actual)


async def test_resolve_feed_records_content_and_model() -> None:
    """
    ARRANGE: feed_pair with successful fetcher and model
    ACT:     call _resolve_feed
    ASSERT:  record models and raw_data match expected values
    """
    feed_pair = (dummy_fetch_success, DummyModel)

    actual = await resolve._resolve_feed(feed_pair)

    expected = [
        (DummyModel, {"alpha": 1}),
        (DummyModel, {"beta": 2}),
    ]

    assert [(record.model, record.raw_data) for record in actual] == expected


async def test_resolve_feed_empty_or_none() -> None:
    """
    ARRANGE: feed_pair with empty fetch actual
    ACT:     call _resolve_feed
    ASSERT:  returns an empty list
    """
    feed_pair = (dummy_fetch_empty, DummyModel)

    actual = await resolve._resolve_feed(feed_pair)

    assert actual == []


async def test_safe_fetch_empty_returns_empty_list() -> None:
    """
    ARRANGE: fetcher returns an empty list
    ACT:     call _safe_fetch
    ASSERT:  returns the empty list, not None
    """
    actual = await _safe_fetch(dummy_fetch_empty, "Dummy", wait_timeout=1.0)

    assert actual == []


async def test_resolve_feed_error_returns_empty_list() -> None:
    """
    ARRANGE: feed_pair whose fetcher always errors
    ACT:     call _resolve_feed
    ASSERT:  returns an empty list
    """
    feed_pair = (dummy_fetch_error, DummyModel)

    actual = await resolve._resolve_feed(feed_pair)

    assert actual == []
