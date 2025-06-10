# exchanges/test_xetra.py

import itertools
import re
from collections.abc import AsyncGenerator
from typing import Any

import pytest
from httpx import Response
from respx import MockRouter

from equity_aggregator.adapters.data_sources.authoritative_feeds.xetra import (
    _build_payload,
    _parse_equities,
    _unique_by_key,
    fetch_equities,
)

pytestmark = pytest.mark.unit


def test_build_payload_offset0() -> None:
    """
    ARRANGE: offset=0, limit=10
    ACT:     call _build_payload
    ASSERT:  payload reflects offset & limit
    """
    offset, limit = 0, 10
    payload = _build_payload(offset, limit)
    assert payload["offset"] == offset and payload["limit"] == limit


def test_build_payload_offset5() -> None:
    """
    ARRANGE: offset=5, limit=25
    ACT:     call _build_payload
    ASSERT:  payload reflects offset & limit
    """
    offset, limit = 5, 25
    payload = _build_payload(offset, limit)
    assert payload["offset"] == offset and payload["limit"] == limit


def test_parse_equities_normalisation() -> None:
    """
    ARRANGE: raw API item with nested name / misc fields
    ACT:     call _parse_equities
    ASSERT:  output dict matches expected normalised values
    """
    raw_items: list[dict[str, Any]] = [
        {
            "name": {"originalValue": "TestCo"},
            "wkn": "W1",
            "isin": "I1",
            "slug": "s1",
            "overview": {"k": "v"},
            "performance": {"p": 1},
            "keyData": {"kd": "data"},
            "sustainability": {"s": True},
        },
    ]
    expected = [
        {
            "name": "TestCo",
            "wkn": "W1",
            "isin": "I1",
            "slug": "s1",
            "mic": "XETR",
            "currency": "EUR",
            "overview": {"k": "v"},
            "performance": {"p": 1},
            "key_data": {"kd": "data"},
            "sustainability": {"s": True},
        },
    ]

    assert _parse_equities(raw_items) == expected


def test_parse_equities_empty() -> None:
    """
    ARRANGE: empty items list
    ACT:     call _parse_equities
    ASSERT:  returns empty list
    """
    assert _parse_equities([]) == []


@pytest.mark.asyncio
async def test_unique_by_key_first_wins() -> None:
    """
    ARRANGE: async generator yields duplicate ISINs
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
async def test_fetch_equities_single_page(respx_mock: MockRouter) -> None:
    """
    ARRANGE: first response recordsTotal <= page_size → no further pages
             PAGE0 → one equity (ISIN_X)
    ACT:     run fetch_equities(page_size=1)
    ASSERT:  single ISIN returned
    """
    page0 = {
        "recordsTotal": 1,
        "data": [{"name": {"originalValue": "A"}, "isin": "ISIN_X"}],
    }

    route = respx_mock.post(
        re.compile(r"https://api\.boerse-frankfurt\.de/v1/search/equity_search"),
    )
    route.side_effect = itertools.cycle([Response(200, json=page0)])

    rows = await fetch_equities(page_size=1, concurrency=2)

    assert {r["isin"] for r in rows} == {"ISIN_X"}


@pytest.mark.asyncio
async def test_fetch_equities_multi_page(respx_mock: MockRouter) -> None:
    """
    ARRANGE: recordsTotal > page_size → crawler requests page-0 & page-1
             - PAGE0 (offset 0) → ISIN_0  , recordsTotal=2
             - PAGE1 (offset 1) → ISIN_1  , recordsTotal ignored
    ACT:     run fetch_equities(page_size=1)
    ASSERT:  two unique ISINs returned
    """
    page0 = {
        "recordsTotal": 2,
        "data": [{"name": {"originalValue": "Z0"}, "isin": "ISIN_0"}],
    }
    page1 = {
        "recordsTotal": 2,
        "data": [{"name": {"originalValue": "Z1"}, "isin": "ISIN_1"}],
    }

    route = respx_mock.post(
        re.compile(r"https://api\.boerse-frankfurt\.de/v1/search/equity_search"),
    )
    route.side_effect = itertools.cycle(
        [
            Response(200, json=page0),
            Response(200, json=page1),
        ],
    )

    rows = await fetch_equities(page_size=1, concurrency=2)

    assert {r["isin"] for r in rows} == {"ISIN_0", "ISIN_1"}
