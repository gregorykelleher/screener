# tests/live/test_exchanges.py

from typing import Any

import httpx
import pytest

from equity_aggregator.adapters import (
    fetch_equity_records_euronext,
    fetch_equity_records_lse,
    fetch_equity_records_xetra,
)

pytestmark = pytest.mark.live

# Map of exchange names to their fetch functions
FEEDS: list[tuple[str, Any]] = [
    ("Euronext", fetch_equity_records_euronext),
    ("LSE", fetch_equity_records_lse),
    ("Xetra", fetch_equity_records_xetra),
]


@pytest.fixture(params=FEEDS, ids=lambda v: v[0])
async def retrieve_feed_rows(
    request: pytest.FixtureRequest,
) -> tuple[str, list[dict[str, Any]]]:
    """
    Retrieve equity rows for a given feed, handling network and server errors
    gracefully.

    Args:
        request (pytest.FixtureRequest): Pytest fixture request containing the feed
            tuple (exchange name, fetch function).

    Returns:
        tuple[str, list[dict[str, Any]]]: Tuple of exchange name and list of rows.
    """
    name, fetch_func = request.param
    server_error_min = 500
    server_error_max = 600

    try:
        rows = await fetch_func(page_size=50, concurrency=4)
    except httpx.HTTPStatusError as error:
        status = error.response.status_code
        if server_error_min <= status < server_error_max:
            pytest.xfail(f"{name} 5xx: {status}")
        raise
    except httpx.HTTPError as error:
        pytest.xfail(f"{name} network error: {error!r}")

    return name, rows


async def test_rows_non_empty(
    retrieve_feed_rows: tuple[str, list[dict[str, Any]]],
) -> None:
    """
    ARRANGE:    Unpack the feed name and rows from the fixture.
    ACT:        Convert the rows list to a boolean to check if it is non-empty.
    ASSERT:     Assert that the result is True, indicating that rows are present.
    """
    # ARRANGE
    name, rows = retrieve_feed_rows

    # ACT
    actual = bool(rows)

    # ASSERT
    assert actual, f"{name}: empty result"


async def test_required_keys(
    retrieve_feed_rows: tuple[str, list[dict[str, Any]]],
) -> None:
    """
    ARRANGE:    Unpack the feed name and rows from the fixture.
    ACT:        Check that each row contains all required keys.
    ASSERT:     Assert that all rows have the required keys.
    """
    # ARRANGE
    name, rows = retrieve_feed_rows

    # ACT
    valid = all(row.keys() >= {"isin", "name", "currency"} for row in rows)

    # ASSERT
    assert valid, f"{name}: some rows missing keys"


async def test_isin_non_empty(
    retrieve_feed_rows: tuple[str, list[dict[str, Any]]],
) -> None:
    """
    ARRANGE:    Unpack the feed name and rows from the fixture.
    ACT:        Check that all rows have a non-empty ISIN value.
    ASSERT:     Assert that no row has an empty ISIN.
    """
    # ARRANGE
    name, rows = retrieve_feed_rows

    # ACT
    all_non_empty = all(row.get("isin") for row in rows)

    # ASSERT
    assert all_non_empty, f"{name}: empty ISIN detected"


async def test_isin_unique(
    retrieve_feed_rows: tuple[str, list[dict[str, Any]]],
) -> None:
    """
    ARRANGE:    Unpack the feed name and rows from the fixture.
    ACT:        Collect ISINs and check if their count matches count of unique ISINs.
    ASSERT:     Assert that all ISINs are unique for the feed.
    """
    # ARRANGE
    name, rows = retrieve_feed_rows

    # ACT
    isins = [row["isin"] for row in rows]
    unique = len(isins) == len(set(isins))

    # ASSERT
    assert unique, f"{name}: duplicate ISINs found"
