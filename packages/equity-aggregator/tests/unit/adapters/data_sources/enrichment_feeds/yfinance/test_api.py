# yfinance/test_api.py


from collections.abc import Callable, Mapping

import httpx
import pytest

from equity_aggregator.adapters.data_sources.enrichment_feeds.yfinance.api import (
    _flatten_module_dicts,
    get_quote_summary,
    pick_best_symbol,
    search_quotes,
)
from equity_aggregator.adapters.data_sources.enrichment_feeds.yfinance.config import (
    FeedConfig,
)
from equity_aggregator.adapters.data_sources.enrichment_feeds.yfinance.session import (
    YFSession,
)

from ._utils import close, make_client

pytestmark = pytest.mark.unit


def _handler_factory(
    pattern_to_response: Mapping[str, httpx.Response],
) -> Callable[[httpx.Request], httpx.Response]:
    """
    Creates handler that returns a predefined httpx.Response based on URL patterns.

    Args:
        pattern_to_response (Mapping[str, httpx.Response]): A mapping of string
            patterns to httpx.Response objects. If a pattern is found in the request
            URL, the corresponding response is returned.

    Returns:
        Callable[[httpx.Request], httpx.Response]: An asynchronous handler function
            that takes an httpx.Request and returns the matching httpx.Response,
            or a default 200 response if no pattern matches.
    """

    async def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        for pattern, response in pattern_to_response.items():
            if pattern in url:
                return response
        return httpx.Response(200)

    return handler


def _make_session(
    patterns: Mapping[str, httpx.Response] | None = None,
) -> YFSession:
    """
    Creates a YFSession instance with a mock HTTPX client for testing.

    Args:
        patterns (Mapping[str, httpx.Response] | None): Optional mapping of URL
            patterns to httpx.Response objects. Used to mock HTTP responses for
            specific endpoints. If None, an empty mapping is used.

    Returns:
        YFSession: A YFSession configured with a mock HTTPX client.
    """
    client = make_client(_handler_factory(patterns or {}))
    return YFSession(FeedConfig(), client)


def test_flatten_module_dicts_merges_and_overwrites() -> None:
    """
    ARRANGE: overlapping module dictionaries
    ACT:     call _flatten_module_dicts
    ASSERT:  later module overwrites earlier duplicate keys
    """
    modules = ("price", "financialData")
    payload = {
        "price": {"currency": "USD", "dup": 1},
        "financialData": {"ebitda": 42, "dup": 2},
    }

    merged = _flatten_module_dicts(modules, payload)

    assert merged == {"currency": "USD", "dup": 2, "ebitda": 42}


def test_pick_best_symbol_returns_expected() -> None:
    """
    ARRANGE: two candidate quotes, one clear winner
    ACT:     call pick_best_symbol
    ASSERT:  winning symbol is returned
    """
    quotes = [
        {"symbol": "AAA", "shortname": "Alpha Corp"},
        {"symbol": "BBB", "shortname": "Beta Company"},
    ]

    best = pick_best_symbol(
        quotes,
        name_key="shortname",
        expected_name="Beta Company",
        expected_symbol="BBB",
    )

    assert best == "BBB"


def test_pick_best_symbol_returns_none_below_threshold() -> None:
    """
    ARRANGE: single candidate with very low fuzzy score
    ACT:     call pick_best_symbol with high threshold
    ASSERT:  None is returned
    """
    quotes = [{"symbol": "XYZ", "shortname": "Irrelevant"}]

    best = pick_best_symbol(
        quotes,
        name_key="shortname",
        expected_name="Target",
        expected_symbol="TGT",
        min_score=300,
    )

    assert best is None


async def test_search_quotes_filters_to_equity() -> None:
    """
    ARRANGE: search endpoint returns EQUITY and non-EQUITY types
    ACT:     call search_quotes
    ASSERT:  only EQUITY quotes are kept
    """
    payload = {
        "quotes": [
            {"symbol": "EQ", "quoteType": "EQUITY"},
            {"symbol": "MF", "quoteType": "MUTUALFUND"},
        ],
    }
    session = _make_session({"finance/search": httpx.Response(200, json=payload)})

    quotes = await search_quotes(session, "whatever")
    await close(session._client)

    assert quotes == [payload["quotes"][0]]


async def test_search_quotes_handles_http_error() -> None:
    """
    ARRANGE: search endpoint replies with HTTP 500
    ACT:     call search_quotes
    ASSERT:  empty list is returned
    """
    session = _make_session({"finance/search": httpx.Response(500)})

    quotes = await search_quotes(session, "boom")
    await close(session._client)

    assert quotes == []


async def test_search_quotes_handles_json_error() -> None:
    """
    ARRANGE: search endpoint returns invalid JSON
    ACT:     call search_quotes
    ASSERT:  empty list is returned
    """
    session = _make_session(
        {"finance/search": httpx.Response(200, content=b"not-json")},
    )

    quotes = await search_quotes(session, "bad-json")
    await close(session._client)

    assert quotes == []


async def test_get_info_flattens_modules() -> None:
    """
    ARRANGE: quoteSummary returns nested price module
    ACT:     call get_info
    ASSERT:  nested keys are flattened
    """
    quote_payload = {"quoteSummary": {"result": [{"price": {"answer": 123}}]}}
    session = _make_session(
        {
            "getcrumb": httpx.Response(200, text='"c"'),
            "quoteSummary": httpx.Response(200, json=quote_payload),
        },
    )

    actual = await get_quote_summary(session, "AAPL", modules=("price",))
    await close(session._client)

    assert actual == {"answer": 123}


async def test_get_info_returns_none_when_no_data() -> None:
    """
    ARRANGE: quoteSummary returns empty result list
    ACT:     call get_info
    ASSERT:  None is returned
    """
    quote_payload = {"quoteSummary": {"result": []}}
    session = _make_session(
        {
            "getcrumb": httpx.Response(200, text='"c"'),
            "quoteSummary": httpx.Response(200, json=quote_payload),
            "finance/quote": httpx.Response(
                200,
                json={"quoteResponse": {"result": []}},
            ),
        },
    )

    actual = await get_quote_summary(session, "EMPTY", modules=("price",))
    await close(session._client)

    assert actual is None


def test_pick_best_symbol_empty_list_returns_none() -> None:
    """
    ARRANGE: empty quotes list
    ACT:     call pick_best_symbol
    ASSERT:  None is returned (branch `if not quotes:`)
    """
    actual = pick_best_symbol(
        [],
        name_key="shortname",
        expected_name="Whatever",
        expected_symbol="WTV",
    )

    assert actual is None


async def test_get_info_fallback_returns_first_result() -> None:
    """
    ARRANGE: quoteSummary returns no data, fallback returns one element.
    ACT:     call get_quote_summary
    ASSERT:  first element from fallback is returned
    """
    empty_quote_summary = {"quoteSummary": {"result": []}}
    fallback_payload = {
        "quoteResponse": {
            "result": [{"answer": 999}],
        },
    }

    session = _make_session(
        {
            "getcrumb": httpx.Response(200, text='"c"'),
            "quoteSummary": httpx.Response(200, json=empty_quote_summary),
            "finance/quote": httpx.Response(200, json=fallback_payload),
        },
    )

    actual = await get_quote_summary(session, "XYZ")
    await close(session._client)

    assert actual == {"answer": 999}
