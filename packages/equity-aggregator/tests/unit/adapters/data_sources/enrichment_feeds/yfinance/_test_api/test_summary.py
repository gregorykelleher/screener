# _test_api/test_summary.py

import asyncio

import httpx
import pytest

from equity_aggregator.adapters.data_sources.enrichment_feeds.yfinance.api.summary import (
    _flatten_module_dicts,
    _get_quote_summary_fallback,
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


async def test_get_quote_summary_raises_for_unexpected_status() -> None:
    """
    ARRANGE: mock 500 response, patch sleep to zero delay
    ACT:     call get_quote_summary
    ASSERT:  LookupError is raised after retries
    """
    real_sleep = asyncio.sleep

    async def _instant(_delay: float) -> None:
        return None

    asyncio.sleep = _instant

    try:

        def handler(request: httpx.Request) -> httpx.Response:
            if "quoteSummary" in str(request.url):
                return httpx.Response(500, json={}, request=request)
            return httpx.Response(200, json={}, request=request)

        session = make_session(handler)

        with pytest.raises(LookupError) as exc:
            await get_quote_summary(session, "MSFT")

        assert "HTTP 500" in str(exc.value)

    finally:
        asyncio.sleep = real_sleep


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


async def test_get_quote_summary_uses_fallback_on_500() -> None:
    """
    ARRANGE: primary quoteSummary endpoint yields HTTP 500,
             fallback endpoint returns one quote dict
    ACT:     call get_quote_summary
    ASSERT:  the dict from the fallback call is returned
    """

    primary_resp = httpx.Response(
        httpx.codes.INTERNAL_SERVER_ERROR,
        request=httpx.Request(
            "GET",
            "https://example.com/v10/finance/quoteSummary/ABC",
        ),
    )

    fallback_payload = {
        "quoteResponse": {"result": [{"currency": "USD", "marketCap": 1_234_000}]},
    }

    fallback_resp = httpx.Response(
        httpx.codes.OK,
        json=fallback_payload,
        request=httpx.Request("GET", "https://example.com/v7/finance/quote"),
    )

    class _Config:
        quote_summary_url = "https://example.com/v10/finance/quoteSummary/"
        quote_summary_fallback_url = "https://example.com/v7/finance/quote"

    class _Session:
        def __init__(self) -> None:
            self.config = _Config()

        async def get(self, url: str, *, params: dict | None = None) -> httpx.Response:
            return (
                primary_resp
                if url.startswith(self.config.quote_summary_url)
                else fallback_resp
            )

    session = _Session()

    actual = await get_quote_summary(session, "ABC", modules=("price",))

    assert actual == {"currency": "USD", "marketCap": 1_234_000}


async def test_get_quote_summary_fallback_returns_none_when_no_results() -> None:
    """
    ARRANGE: fallback endpoint returns empty result array
    ACT:     call _get_quote_summary_fallback directly
    ASSERT:  function returns None
    """
    empty_resp = httpx.Response(
        httpx.codes.OK,
        json={"quoteResponse": {"result": []}},
        request=httpx.Request("GET", "https://example.com/v7/finance/quote"),
    )

    class _Config:
        quote_summary_fallback_url = "https://example.com/v7/finance/quote"

    class _Session:
        def __init__(self) -> None:
            self.config = _Config()

        async def get(self, _url: str, *, params: dict | None = None) -> httpx.Response:
            return empty_resp

    session = _Session()
    actual = await _get_quote_summary_fallback(session, "EMPTY")

    assert actual is None


async def test_get_quote_summary_raises_lookup_error_on_429() -> None:
    """
    ARRANGE: stub session that always returns HTTP 429
    ACT:     invoke get_quote_summary
    ASSERT:  LookupError with 'HTTP 429' is raised
    """
    resp_429 = httpx.Response(
        httpx.codes.TOO_MANY_REQUESTS,
        request=httpx.Request(
            "GET",
            "https://example.com/v10/finance/quoteSummary/XYZ",
        ),
    )

    class _Config:
        modules = ("price",)
        quote_summary_url = "https://example.com/v10/finance/quoteSummary/"

    class _Session:
        def __init__(self) -> None:
            self.config = _Config()

        async def get(self, _url: str, *, params: dict | None = None) -> httpx.Response:
            return resp_429

    session = _Session()

    with pytest.raises(LookupError) as exc_info:
        await get_quote_summary(session, "XYZ", modules=("price",))

    assert "HTTP 429" in str(exc_info.value)
