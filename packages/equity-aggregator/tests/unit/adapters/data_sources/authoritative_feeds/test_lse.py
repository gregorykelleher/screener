# exchanges/test_lse.py

import itertools
import re
from collections.abc import AsyncGenerator

import pytest
from httpx import Response
from respx import MockRouter

from equity_aggregator.adapters.data_sources.authoritative_feeds.lse import (
    _build_payload,
    _parse_equities,
    _unique_by_key,
    fetch_equity_records,
)

# pytestmark = pytest.mark.unit


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
            },
        ],
    }
    assert _parse_equities(sample) == ([{"isin": "X1"}], 2)


def test_parse_equities_empty() -> None:
    """
    ARRANGE: JSON without target component
    ACT:     call _parse_equities
    ASSERT:  returns ([], None)
    """
    assert _parse_equities({"content": []}) == ([], None)


@pytest.mark.asyncio
async def test_unique_by_key_first_wins() -> None:
    """
    ARRANGE: async source yields duplicate ISINs
    ACT:     call _unique_by_key
    ASSERT:  only first occurrence kept
    """

    async def src() -> AsyncGenerator[dict[str, str], None]:
        for isin in ["A", "B", "A", "C"]:
            yield {"isin": isin}

    actual = [
        item async for item in _unique_by_key(src(), key_func=lambda d: d["isin"])
    ]
    assert actual == [{"isin": "A"}, {"isin": "B"}, {"isin": "C"}]


@pytest.mark.asyncio
async def test_fetch_equities_pagination_known_total(respx_mock: MockRouter) -> None:
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
                },
            ],
        },
    ]
    page1 = [
        {
            "content": [
                {
                    "name": "priceexplorersearch",
                    "value": {"content": [{"isin": "ISIN_B"}], "totalPages": 2},
                },
            ],
        },
    ]

    route = respx_mock.post(
        re.compile(r"https://api\.londonstockexchange\.com/api/v1/components/refresh"),
    )
    route.side_effect = itertools.cycle(
        [Response(200, json=page0), Response(200, json=page1)],
    )

    rows = await fetch_equities(page_size=1, concurrency=2)

    assert {r["isin"] for r in rows} == {"ISIN_A", "ISIN_B"}


@pytest.mark.asyncio
async def test_fetch_equities_fallback_serial_loop(respx_mock: MockRouter) -> None:
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
                },
            ],
        },
    ]
    page1 = [
        {
            "content": [
                {
                    "name": "priceexplorersearch",
                    "value": {"content": [{"isin": "ISIN_D"}], "totalPages": None},
                },
            ],
        },
    ]
    empty = [
        {
            "content": [
                {
                    "name": "priceexplorersearch",
                    "value": {"content": [], "totalPages": None},
                },
            ],
        },
    ]

    route = respx_mock.post(
        re.compile(r"https://api\.londonstockexchange\.com/api/v1/components/refresh"),
    )
    route.side_effect = itertools.cycle(
        [
            Response(200, json=page0),
            Response(200, json=page1),
            Response(200, json=empty),
        ],
    )

    rows = await fetch_equities(page_size=1, concurrency=2)

    assert {r["isin"] for r in rows} == {"ISIN_C", "ISIN_D"}
