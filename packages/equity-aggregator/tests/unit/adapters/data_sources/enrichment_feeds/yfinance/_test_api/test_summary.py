# _test_api/test_summary.py

import asyncio

import httpx
import pytest

from equity_aggregator.adapters.data_sources.enrichment_feeds.yfinance.api.summary import (
    _flatten_module_dicts,
    get_quote_summary,
)
from tests.unit.adapters.data_sources.enrichment_feeds.yfinance._helpers import (
    make_session,
)

pytestmark = pytest.mark.unit


def test_flatten_module_dicts_merges_and_overwrites() -> None:
    """
    ARRANGE: two overlapping module dictionaries
    ACT:     call _flatten_module_dicts
    ASSERT:  keys from later module overwrite earlier ones
    """

    modules = ("price", "summaryDetail")
    payload = {
        "price": {"currency": "USD", "regularMarketPrice": 100},
        "summaryDetail": {"currency": "GBP", "dividendYield": 0.02},
    }

    merged = _flatten_module_dicts(modules, payload)

    assert merged == {
        "currency": "GBP",
        "regularMarketPrice": 100,
        "dividendYield": 0.02,
    }


async def test_get_quote_summary_returns_flattened_data_on_success() -> None:
    """
    ARRANGE: quoteSummary endpoint returns two modules
    ACT:     call get_quote_summary
    ASSERT:  flattened dict is returned
    """

    raw = {
        "quoteSummary": {
            "result": [
                {
                    "price": {"regularMarketPrice": 150},
                    "summaryDetail": {"marketCap": 2_000_000_000},
                },
            ],
        },
    }
    session = make_session(lambda r: httpx.Response(200, json=raw, request=r))

    actual = await get_quote_summary(
        session,
        "AAPL",
        modules=("price", "summaryDetail"),
    )

    assert actual == {"regularMarketPrice": 150, "marketCap": 2_000_000_000}


async def test_get_quote_summary_uses_fallback_on_500() -> None:
    """
    ARRANGE: main endpoint 500, fallback returns one quote
    ACT:     call get_quote_summary
    ASSERT:  fallback quote dict is returned
    """

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if "quoteSummary" in url:
            return httpx.Response(500, json={}, request=request)
        if "quote" in url:
            payload = {
                "quoteResponse": {
                    "result": [{"symbol": "MSFT", "regularMarketPrice": 100}],
                },
            }
            return httpx.Response(200, json=payload, request=request)
        return httpx.Response(200, json={}, request=request)

    session = make_session(handler)

    actual = await get_quote_summary(session, "MSFT")

    assert actual == {"symbol": "MSFT", "regularMarketPrice": 100}


async def test_get_quote_summary_raises_lookup_when_empty_result() -> None:
    """
    ARRANGE: quoteSummary returns empty result array
    ACT:     call get_quote_summary
    ASSERT:  LookupError is raised
    """
    raw = {"quoteSummary": {"result": []}}
    session = make_session(lambda r: httpx.Response(200, json=raw, request=r))

    with pytest.raises(LookupError):
        await get_quote_summary(session, "NFLX")


async def test_get_quote_summary_raises_on_429() -> None:
    """
    ARRANGE: session.get always returns HTTP 429; asyncio.sleep patched to no-op
    ACT:     call get_quote_summary
    ASSERT:  LookupError with 'HTTP 429' is raised
    """

    real_sleep = asyncio.sleep

    async def _instant(_delay: float) -> None:
        return None

    asyncio.sleep = _instant

    try:
        session = make_session(lambda r: httpx.Response(429, json={}, request=r))

        with pytest.raises(LookupError) as exc:
            await get_quote_summary(session, "TICK")

        assert "HTTP 429" in str(exc.value)
    finally:
        asyncio.sleep = real_sleep
