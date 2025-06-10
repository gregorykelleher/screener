# exchanges/test_euronext.py

import itertools
import re
from collections.abc import AsyncGenerator

import pytest
from httpx import Response
from respx import MockRouter

from equity_aggregator.adapters.data_sources.authoritative_feeds.euronext import (
    _build_payload,
    _parse_equities,
    _unique_by_key,
    fetch_equities,
)

pytestmark = pytest.mark.unit


def test_build_payload_page0() -> None:
    """
    ARRANGE: start=0, length=10, draw=1
    ACT:     call _build_payload
    ASSERT:  mapping equals expected dict
    """
    start, length, draw = 0, 10, 1
    expected = {
        "draw": 1,
        "start": 0,
        "length": 10,
        "iDisplayLength": 10,
        "iDisplayStart": 0,
    }

    actual = _build_payload(start, length, draw)

    assert actual == expected


def test_build_payload_offset5() -> None:
    """
    ARRANGE: start=5, length=50, draw=3
    ACT:     call _build_payload
    ASSERT:  mapping equals expected dict
    """
    start, length, draw = 5, 50, 3
    expected = {
        "draw": 3,
        "start": 5,
        "length": 50,
        "iDisplayLength": 50,
        "iDisplayStart": 5,
    }

    actual = _build_payload(start, length, draw)

    assert actual == expected


def test_parse_equities_html_row() -> None:
    """
    ARRANGE: row with HTML name tag + two comma-separated MICs
    ACT:     call _parse_equities
    ASSERT:  fields normalised & extracted
    """
    aa_data = [
        [
            "x",
            "<a>Acme Corp</a>",
            "ISIN123",
            "SYM",
            "<div>XPAR,XAMS</div>",
            "<div>EUR <span>100.50</span></div>",
        ],
    ]
    expected = [
        {
            "name": "Acme Corp",
            "symbol": "SYM",
            "isin": "ISIN123",
            "mics": ["XPAR", "XAMS"],
            "currency": "EUR",
            "last_price": "100.50",
        },
    ]

    actual = list(_parse_equities(aa_data))

    assert actual == expected


def test_parse_equities_plain_row() -> None:
    """
    ARRANGE: row with plaintext name + no MICs
    ACT:     call _parse_equities
    ASSERT:  empty MIC list and correct field extraction
    """
    aa_data = [
        [
            "",
            "NoTagName",
            "ZZZ",
            "S",
            "",
            "<div>USD <span>10</span></div>",
        ],
    ]
    expected = [
        {
            "name": "NoTagName",
            "symbol": "S",
            "isin": "ZZZ",
            "mics": [],
            "currency": "USD",
            "last_price": "10",
        },
    ]

    actual = list(_parse_equities(aa_data))

    assert actual == expected


@pytest.mark.asyncio
async def test_unique_by_key_first_wins() -> None:
    """
    ARRANGE: async source yields duplicate keys
    ACT:     call _unique_by_key
    ASSERT:  only first occurrence kept (order preserved)
    """

    async def src() -> AsyncGenerator[dict[str, int], None]:
        for k in [1, 2, 1, 3, 2]:
            yield {"key": k}

    actual = [item async for item in _unique_by_key(src(), key_func=lambda d: d["key"])]

    assert actual == [{"key": 1}, {"key": 2}, {"key": 3}]


@pytest.mark.asyncio
async def test_fetch_equities_pagination_two_pages(respx_mock: MockRouter) -> None:
    """
    ARRANGE: every POST alternates PAGE0→PAGE1 via respx cycle
    ACT:     run fetch_equities with page_size=1
    ASSERT:  deduped result contains exactly two ISINs
    """
    page0 = {
        "aaData": [
            [
                "",
                "<a>P0</a>",
                "ISIN0",
                "SYM0",
                "<div>XPAR</div>",
                "<div>EUR <span>1</span></div>",
            ],
        ],
        "iTotalRecords": 2,
    }
    page1 = {
        "aaData": [
            [
                "",
                "<a>P1</a>",
                "ISIN1",
                "SYM1",
                "<div>XPAR</div>",
                "<div>EUR <span>2</span></div>",
            ],
        ],
        "iTotalRecords": 2,
    }

    route = respx_mock.post(
        re.compile(r"https://live\.euronext\.com/en/pd_es/data/stocks\?mics=.*"),
    )
    route.side_effect = itertools.cycle(
        [Response(200, json=page0), Response(200, json=page1)],
    )

    rows = await fetch_equities(page_size=1, concurrency=2)

    assert {r["isin"] for r in rows} == {"ISIN0", "ISIN1"}
