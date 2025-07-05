# yfinance/test_feed.py


import asyncio

import httpx
import pytest

from equity_aggregator.adapters.data_sources._cache._cache import (
    load_cache_entry,
    save_cache_entry,
)
from equity_aggregator.adapters.data_sources.enrichment_feeds.yfinance.api import (
    pick_best_symbol,
)
from equity_aggregator.adapters.data_sources.enrichment_feeds.yfinance.feed import (
    YFinanceFeed,
)

from ._utils import close, handler_factory, make_session

pytestmark = pytest.mark.unit


def test_pick_best_symbol_returns_expected() -> None:
    """
    ARRANGE: two candidate quotes where one clearly matches inputs
    ACT:     call pick_best_symbol
    ASSERT:  matching symbol is returned
    """
    quotes = [
        {"symbol": "AAA", "shortname": "Alpha Corp"},
        {"symbol": "BBB", "shortname": "Beta Company"},
    ]

    actual = pick_best_symbol(
        quotes,
        name_key="shortname",
        expected_name="Beta Company",
        expected_symbol="BBB",
    )

    assert actual == "BBB"


async def test_find_by_identifier_no_viable_quotes_returns_none() -> None:
    """
    ARRANGE: search returns quotes missing required fields
    ACT:     call _find_by_identifier
    ASSERT:  returns None when no viable quotes found
    """
    search_payload = {
        "quotes": [
            {"symbol": "", "longname": ""},
            {"symbol": None, "longname": "NameOnly"},
            {"symbol": "SymOnly", "longname": None},
        ],
    }
    handler = handler_factory(
        {"finance/search": httpx.Response(200, json=search_payload)},
    )
    session = make_session(handler)
    feed = YFinanceFeed(session)

    actual = await feed._find_by_identifier("ID", "Name", "SYM")
    await close(session._client)

    assert actual is None


async def test_find_by_identifier_single_viable_returns_info() -> None:
    """
    ARRANGE: identifier search yields a single viable quote
    ACT: call _find_by_identifier
    ASSERT: flattened info dict is returned
    """

    search_payload = {
        "quotes": [
            {"symbol": "AAPL", "longname": "Apple Inc", "quoteType": "EQUITY"},
        ],
    }
    quote_payload = {
        "quoteSummary": {"result": [{"price": {"regularMarketPrice": 150}}]},
    }

    patterns = {
        "finance/search": httpx.Response(200, json=search_payload),
        "getcrumb": httpx.Response(200, text='"crumb"'),
        "quoteSummary": httpx.Response(200, json=quote_payload),
    }

    expected_regular_market_price = 150

    session = make_session(handler_factory(patterns))
    feed = YFinanceFeed(session)

    actual = await feed._find_by_identifier("id", "Apple Inc", "AAPL")
    await close(session._client)

    assert actual["regularMarketPrice"] == expected_regular_market_price


async def test_fetch_equity_uses_cache_after_first_call() -> None:
    """
    ARRANGE: make two fetches for same symbol
    ACT:     perform fetch twice
    ASSERT:  second result equals first (served from cache, not network)
    """
    search_payload = {
        "quotes": [{"symbol": "CST", "longname": "Cache-Test", "quoteType": "EQUITY"}],
    }
    quote_payload = {"quoteSummary": {"result": [{"price": {"value": 1}}]}}

    patterns = {
        "finance/search": httpx.Response(200, json=search_payload),
        "getcrumb": httpx.Response(200, text='"c"'),
        "quoteSummary": httpx.Response(200, json=quote_payload),
    }

    session = make_session(handler_factory(patterns))
    feed = YFinanceFeed(session)

    first = await feed.fetch_equity(symbol="CST", name="Cache-Test")
    second = await feed.fetch_equity(symbol="CST", name="Cache-Test")
    await close(session._client)

    assert first == second


def test_fetch_equity_returns_cached_without_network() -> None:
    """
    ARRANGE: seed disk-cache, stub client that would error if called
    ACT:     call fetch_equity
    ASSERT:  cached record is returned (no HTTP request performed)
    """
    cached_record = {"symbol": "CCH", "metric": 42}
    save_cache_entry("yfinance_equities", "CCH", cached_record)

    # Any network request should fail if hit
    def _boom(_: httpx.Request) -> httpx.Response:
        return httpx.Response(500)

    session = make_session(_boom)
    feed = YFinanceFeed(session)

    actual = asyncio.run(feed.fetch_equity(symbol="CCH", name="Cached Corp"))

    assert actual == cached_record


async def test_find_via_name_symbol_low_score_returns_none() -> None:
    """
    ARRANGE: Yahoo search returns mismatching quote
    ACT:     call _find_via_name_symbol
    ASSERT:  None is returned when fuzzy score < threshold
    """
    search_payload = {
        "quotes": [
            {"symbol": "BAD", "shortname": "Wrong Co", "quoteType": "EQUITY"},
        ],
    }

    handler = handler_factory(
        {"finance/search": httpx.Response(200, json=search_payload)},
    )
    session = make_session(handler)
    feed = YFinanceFeed(session)

    actual = await feed._find_via_name_symbol("Right Name", "RGT")
    await close(session._client)

    assert actual is None


async def test_fetch_equity_no_data_returns_none_and_skips_cache() -> None:
    """
    ARRANGE: Yahoo returns no viable quotes → _retrieve yields None
    ACT:     call fetch_equity
    ASSERT:  result is None (branch where `if yf_record:` is *false*)
    """
    session = make_session(  # search endpoint returns empty quotes list
        handler_factory({"finance/search": httpx.Response(200, json={"quotes": []})}),
    )
    feed = YFinanceFeed(session)

    actual = await feed.fetch_equity(symbol="MISS", name="Missing Inc")
    await close(session._client)

    assert actual is None and load_cache_entry("yfinance_equities", "MISS") is None


async def test_find_by_identifier_multiple_viable_selects_best() -> None:
    """
    ARRANGE: identifier search yields two viable quotes
    ACT:     call _find_by_identifier
    ASSERT:  branch where len(viable) > 1 and a best symbol is chosen executes
    """
    search_payload = {
        "quotes": [
            {"symbol": "WRNG", "longname": "Wrong Ltd", "quoteType": "EQUITY"},
            {"symbol": "BEST", "longname": "Best Plc", "quoteType": "EQUITY"},
        ],
    }

    quote_payload = {"quoteSummary": {"result": [{"price": {"metric": 7}}]}}

    patterns = {
        "finance/search": httpx.Response(200, json=search_payload),
        "getcrumb": httpx.Response(200, text='"x"'),
        "quoteSummary/BEST": httpx.Response(200, json=quote_payload),
    }

    session = make_session(handler_factory(patterns))
    feed = YFinanceFeed(session)

    expected_metric = 7

    info = await feed._find_by_identifier("010101", "Best Plc", "BEST")
    await close(session._client)

    assert info["metric"] == expected_metric


async def test_find_by_identifier_multiple_viable_none_selected_returns_none() -> None:
    """
    ARRANGE: override _pick_best_symbol to return None
    ACT:     call _find_by_identifier
    ASSERT:  early-return branch when `chosen` is None executes
    """

    search_payload = {
        "quotes": [
            {"symbol": "AAA", "longname": "Alpha", "quoteType": "EQUITY"},
            {"symbol": "BBB", "longname": "Beta", "quoteType": "EQUITY"},
        ],
    }
    session = make_session(
        handler_factory({"finance/search": httpx.Response(200, json=search_payload)}),
    )
    feed = YFinanceFeed(session)

    actual = await feed._find_by_identifier("ID123", "Gamma Ltd", "GMM")
    await close(session._client)

    assert actual is None


async def test_find_via_name_symbol_no_quotes_returns_none() -> None:
    """
    ARRANGE: search returns zero quotes
    ACT:     call _find_via_name_symbol
    ASSERT:  branch where `if not quotes:` hits and None is returned
    """
    session = make_session(
        handler_factory({"finance/search": httpx.Response(200, json={"quotes": []})}),
    )
    feed = YFinanceFeed(session)

    actual = await feed._find_via_name_symbol("Nobody Co", "NONE")
    await close(session._client)

    assert actual is None


async def test_fetch_equity_hits_cache_write_branch() -> None:
    """
    ARRANGE: subclass whose _retrieve_yf_equity_data always returns a record
    ACT:     call fetch_equity
    ASSERT:  record is persisted to cache (covers lines 142-144)
    """

    class _AlwaysSucceedsFeed(YFinanceFeed):
        async def _retrieve_yf_equity_data(self, **_: object) -> dict:
            return {"symbol": "COV", "metric": 99}

    # network never reached, but a session is still required
    session = make_session(lambda _: httpx.Response(204))

    feed = _AlwaysSucceedsFeed(session)
    record = await feed.fetch_equity(symbol="COV", name="Covered Corp")
    await close(session._client)

    assert load_cache_entry("yfinance_equities", "COV") == record


async def test_retrieve_yf_equity_data_breaks_after_first_success() -> None:
    """
    ARRANGE: first identifier search succeeds via overridden method
    ACT:     call _retrieve_yf_equity_data
    ASSERT:  success returned immediately (covers lines 196-197)
    """

    class _FirstHitFeed(YFinanceFeed):
        async def _find_by_identifier(self, *_: object, **__: object) -> dict:
            return {"symbol": "WIN"}

        async def _find_via_name_symbol(self, *_: object, **__: object) -> None:
            raise AssertionError("Should not be reached")

    session = make_session(lambda _: httpx.Response(204))

    feed = _FirstHitFeed(session)
    result = await feed._retrieve_yf_equity_data("Winner Co", "WIN", isin="ISIN123")
    await close(session._client)

    assert result == {"symbol": "WIN"}


async def test_find_via_name_symbol_match_returns_info() -> None:
    """
    ARRANGE: search returns a quote whose shortname/symbol match inputs closely
    ACT:     call _find_via_name_symbol
    ASSERT:  info dict from quoteSummary is returned (covers `chosen` branch)
    """
    search_payload = {
        "quotes": [
            {
                "symbol": "GOOD",
                "shortname": "Good Co",
                "quoteType": "EQUITY",
            },
        ],
    }
    quote_payload = {
        "quoteSummary": {"result": [{"price": {"answer": 123}}]},
    }
    patterns = {
        "finance/search": httpx.Response(200, json=search_payload),
        "getcrumb": httpx.Response(200, text='"t"'),
        "quoteSummary": httpx.Response(200, json=quote_payload),
    }

    session = make_session(handler_factory(patterns))
    feed = YFinanceFeed(session)

    expected_answer = 123

    info = await feed._find_via_name_symbol("Good Co", "GOOD")
    await close(session._client)

    assert info["answer"] == expected_answer
