# exchanges/test_lse.py

import itertools
import re
from typing import AsyncGenerator, Dict

import pytest
from httpx import Response

from equity_aggregator.adapters.equity_data_sources.exchanges.lse.fetch_equities import (
    _build_payload,
    _parse_equities,
    _unique_by_key,
    fetch_equities,
)

pytestmark = pytest.mark.unit


# ------------------------------------------------------------------ build‑payload: page‑0
def test_build_payload_page0() -> None:
    """
    ARRANGE: page=0, size=20
    ACT:     call _build_payload
    ASSERT:  encoded page and size parameters present
    """
    page, size = 0, 20
    payload = _build_payload(page, size)
    assert (
        f"page%3D{page}" in payload["parameters"]
        and f"size={size}" in payload["components"][0]["parameters"]
    )


# ------------------------------------------------------------------ build‑payload: offset‑3
def test_build_payload_offset3() -> None:
    """
    ARRANGE: page=3, size=50
    ACT:     call _build_payload
    ASSERT:  encoded page and size parameters present
    """
    page, size = 3, 50
    payload = _build_payload(page, size)
    comp = payload["components"][0]["parameters"]
    assert (
        f"page%3D{page}" in payload["parameters"]
        and f"page={page}" in comp
        and f"size={size}" in comp
    )


# ------------------------------------------------------------------ parse‑equities: component present
def test_parse_equities_with_content() -> None:
    """
    ARRANGE: JSON contains 'priceexplorersearch' component
    ACT:     call _parse_equities
    ASSERT:  returns ([…], total_pages)
    """
    sample = {
        "content": [
            {
                "name": "priceexplorersearch",
                "value": {"content": [{"isin": "X1"}], "totalPages": 2},
            }
        ]
    }
    assert _parse_equities(sample) == ([{"isin": "X1"}], 2)


# ------------------------------------------------------------------ parse‑equities: component missing
def test_parse_equities_empty() -> None:
    """
    ARRANGE: JSON without target component
    ACT:     call _parse_equities
    ASSERT:  returns ([], None)
    """
    assert _parse_equities({"content": []}) == ([], None)


# ------------------------------------------------------------------ deduplication helper
@pytest.mark.asyncio
async def test_unique_by_key_first_wins() -> None:
    """
    ARRANGE: async source yields duplicate ISINs
    ACT:     call _unique_by_key
    ASSERT:  only first occurrence kept
    """

    async def src() -> AsyncGenerator[Dict[str, str], None]:
        for isin in ["A", "B", "A", "C"]:
            yield {"isin": isin}

    actual = [
        item async for item in _unique_by_key(src(), key_func=lambda d: d["isin"])
    ]
    assert actual == [{"isin": "A"}, {"isin": "B"}, {"isin": "C"}]


# ------------------------------------------------------------------ pagination branch: totalPages known
@pytest.mark.asyncio
async def test_fetch_equities_pagination_known_total(respx_mock) -> None:
    """
    ARRANGE: first response says totalPages=2 → crawler requests page-0 & page-1
             respx cycles PAGE0↔PAGE1 indefinitely
    ACT:     run fetch_equities(page_size=1)
    ASSERT:  two unique ISINs returned
    """
    page0 = [
        {
            "content": [
                {
                    "name": "priceexplorersearch",
                    "value": {"content": [{"isin": "ISIN_A"}], "totalPages": 2},
                }
            ]
        }
    ]
    page1 = [
        {
            "content": [
                {
                    "name": "priceexplorersearch",
                    "value": {"content": [{"isin": "ISIN_B"}], "totalPages": 2},
                }
            ]
        }
    ]

    route = respx_mock.post(
        re.compile(r"https://api\.londonstockexchange\.com/api/v1/components/refresh")
    )
    route.side_effect = itertools.cycle(
        [Response(200, json=page0), Response(200, json=page1)]
    )

    rows = await fetch_equities(page_size=1, concurrency=2, use_cache=False)

    assert {r["isin"] for r in rows} == {"ISIN_A", "ISIN_B"}


# ------------------------------------------------------------------ fallback branch: totalPages unknown
@pytest.mark.asyncio
async def test_fetch_equities_fallback_serial_loop(respx_mock) -> None:
    """
    ARRANGE: first response has totalPages=None → triggers while-loop
             - PAGE0  (page=0) → ISIN_C, totalPages=None
             - PAGE1  (page=1) → ISIN_D, totalPages=None
             - PAGE2  (page=2) → empty list      → loop terminates
    ACT:     run fetch_equities(page_size=1)
    ASSERT:  two unique ISINs returned
    """
    page0 = [
        {
            "content": [
                {
                    "name": "priceexplorersearch",
                    "value": {"content": [{"isin": "ISIN_C"}], "totalPages": None},
                }
            ]
        }
    ]
    page1 = [
        {
            "content": [
                {
                    "name": "priceexplorersearch",
                    "value": {"content": [{"isin": "ISIN_D"}], "totalPages": None},
                }
            ]
        }
    ]
    empty = [
        {
            "content": [
                {
                    "name": "priceexplorersearch",
                    "value": {"content": [], "totalPages": None},
                }
            ]
        }
    ]

    route = respx_mock.post(
        re.compile(r"https://api\.londonstockexchange\.com/api/v1/components/refresh")
    )
    route.side_effect = itertools.cycle(
        [
            Response(200, json=page0),
            Response(200, json=page1),
            Response(200, json=empty),
        ]
    )

    rows = await fetch_equities(page_size=1, concurrency=2, use_cache=False)

    assert {r["isin"] for r in rows} == {"ISIN_C", "ISIN_D"}
