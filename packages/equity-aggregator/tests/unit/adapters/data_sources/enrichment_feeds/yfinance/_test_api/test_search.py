# _test_api/test_search.py

import asyncio

import httpx
import pytest

from equity_aggregator.adapters.data_sources.enrichment_feeds.yfinance.api.search import (
    search_quotes,
)
from tests.unit.adapters.data_sources.enrichment_feeds.yfinance._helpers import (
    make_session,
)

pytestmark = pytest.mark.unit


async def test_search_quotes_filters_non_equity() -> None:
    """
    ARRANGE: search response with mixed quoteTypes
    ACT:     call search_quotes
    ASSERT:  only EQUITY quotes remain
    """
    payload = {
        "quotes": [
            {"symbol": "MSFT", "quoteType": "EQUITY"},
            {"symbol": "MSFTA", "quoteType": "ETF"},
            {"symbol": "AAPL", "quoteType": "EQUITY"},
        ],
    }

    excepted_quote_count = 2

    session = make_session(lambda r: httpx.Response(200, json=payload, request=r))

    actual = await search_quotes(session, "msft")

    assert len(actual) == excepted_quote_count


async def test_search_quotes_handles_missing_quotes_field() -> None:
    """
    ARRANGE: 200 response without "quotes" key
    ACT:     call search_quotes
    ASSERT:  returns empty list
    """
    session = make_session(lambda r: httpx.Response(200, json={}, request=r))

    actual = await search_quotes(session, "something")

    assert actual == []


async def test_search_quotes_raises_for_unexpected_status() -> None:
    """
    ARRANGE: mock 500 response
    ACT:     call search_quotes
    ASSERT:  HTTPStatusError is raised
    """
    session = make_session(lambda r: httpx.Response(500, json={}, request=r))

    with pytest.raises(httpx.HTTPStatusError):
        await search_quotes(session, "fail")


async def test_search_quotes_returns_empty_on_429() -> None:
    """
    ARRANGE: session.get always returns HTTP 429; asyncio.sleep patched to zero
    ACT:     call search_quotes
    ASSERT:  empty list is returned
    """
    real_sleep = asyncio.sleep

    async def _instant(_delay: float) -> None:
        return None

    asyncio.sleep = _instant

    try:
        session = make_session(lambda r: httpx.Response(429, json={}, request=r))

        actual = await search_quotes(session, "QUERY429")

        assert actual == []
    finally:
        asyncio.sleep = real_sleep
