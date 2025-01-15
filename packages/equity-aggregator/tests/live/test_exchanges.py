# tests/live/test_exchanges.py

import httpx
import pytest
from typing import Any, Dict, List, Tuple

from equity_aggregator.adapters import (
    fetch_equities_euronext,
    fetch_equities_lse,
    fetch_equities_xetra,
)

pytestmark = pytest.mark.live

# --- constants -------------------------------------------------------------

_REQUIRED_KEYS = {"isin", "name", "currency"}

# Map of exchange names to their fetch functions
VENDORS: List[tuple[str, Any]] = [
    ("Euronext", fetch_equities_euronext),
    ("LSE", fetch_equities_lse),
    ("Xetra", fetch_equities_xetra),
]

# --- fixtures --------------------------------------------------------------


@pytest.fixture(params=VENDORS, ids=lambda v: v[0])
async def retrieve_vendor_rows(request) -> Tuple[str, List[Dict[str, Any]]]:
    """
    Fetch rows for each vendor, handling upstream flakiness.
    Returns (exchange_name, rows).
    """
    name, fetch_func = request.param
    try:
        rows = await fetch_func(page_size=50, concurrency=4)
    except httpx.HTTPStatusError as exc:
        if 500 <= exc.response.status_code < 600:
            pytest.xfail(f"{name} 5xx: {exc.response.status_code}")
        raise
    except httpx.HTTPError as exc:
        pytest.xfail(f"{name} network error: {exc!r}")
    return name, rows


# --- tests -----------------------------------------------------------------


async def test_rows_non_empty(
    retrieve_vendor_rows: Tuple[str, List[Dict[str, Any]]],
) -> None:
    """
    Assert that fetch returns a non-empty list.
    """

    # ARRANGE
    name, rows = retrieve_vendor_rows

    # ACT
    result = bool(rows)

    # ASSERT
    assert result, f"{name}: empty result"


async def test_required_keys(
    retrieve_vendor_rows: Tuple[str, List[Dict[str, Any]]],
) -> None:
    """
    Assert each row has all required keys.
    """

    # ARRANGE
    name, rows = retrieve_vendor_rows

    # ACT
    valid = all(_REQUIRED_KEYS <= row.keys() for row in rows)

    # ASSERT
    assert valid, f"{name}: some rows missing keys"


async def test_isin_non_empty(
    retrieve_vendor_rows: Tuple[str, List[Dict[str, Any]]],
) -> None:
    """
    Assert no row has an empty ISIN.
    """

    # ARRANGE
    name, rows = retrieve_vendor_rows

    # ACT
    all_non_empty = all(row.get("isin") for row in rows)

    # ASSERT
    assert all_non_empty, f"{name}: empty ISIN detected"


async def test_isin_unique(
    retrieve_vendor_rows: Tuple[str, List[Dict[str, Any]]],
) -> None:
    """
    Assert all ISINs are unique per vendor.
    """

    # ARRANGE
    name, rows = retrieve_vendor_rows

    # ACT
    isins = [row["isin"] for row in rows]
    unique = len(isins) == len(set(isins))

    # ASSERT
    assert unique, f"{name}: duplicate ISINs found"
