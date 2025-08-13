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
    ARRANGE: mock 500 response, patch sleep to zero delay
    ACT:     call search_quotes
    ASSERT:  LookupError is raised after retries
    """
    real_sleep = asyncio.sleep

    async def _instant(_delay: float) -> None:
        return None

    asyncio.sleep = _instant

    try:
        session = make_session(lambda r: httpx.Response(500, json={}, request=r))

        with pytest.raises(LookupError):
            await search_quotes(session, "fail")
    finally:
        asyncio.sleep = real_sleep


async def test_search_quotes_raises_for_too_many_requests() -> None:
    """
    ARRANGE: mock 429 response; patch sleep to zero delay
    ACT:     call search_quotes
    ASSERT:  LookupError is raised after retries
    """
    real_sleep = asyncio.sleep

    async def _instant(_delay: float) -> None:
        return None

    asyncio.sleep = _instant

    try:
        session = make_session(lambda r: httpx.Response(429, json={}, request=r))

        with pytest.raises(LookupError):
            await search_quotes(session, "QUERY429")
    finally:
        asyncio.sleep = real_sleep


async def test_search_quotes_returns_empty_list_on_429() -> None:
    """
    ARRANGE: session stub that always yields a 429 response
    ACT:     invoke search_quotes
    ASSERT:  an empty list is returned
    """

    response_429 = httpx.Response(
        httpx.codes.TOO_MANY_REQUESTS,
        request=httpx.Request("GET", "https://example.com/search"),
        json={},
    )

    class _StubConfig:
        search_url = "https://example.com/search"

    class _StubSession:
        def __init__(self, resp: httpx.Response) -> None:
            self._resp = resp
            self.config = _StubConfig()

        async def get(self, _url: str, *, params: dict | None = None) -> httpx.Response:
            return self._resp

    session = _StubSession(response_429)

    actual = await search_quotes(session, "throttled")

    assert actual == []
