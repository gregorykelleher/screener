# yfinance/test_feed.py


import asyncio

import httpx
import pytest

from equity_aggregator.adapters.data_sources.enrichment_feeds.yfinance.feed import (
    YFinanceFeed,
    _choose_symbol,
)
from equity_aggregator.adapters.data_sources.enrichment_feeds.yfinance.utils import (
    pick_best_symbol,
)
from equity_aggregator.storage import (
    load_cache_entry,
    save_cache_entry,
)

from ._helpers import close, handler_factory, make_session

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


def test_choose_symbol_identical_names_returns_first() -> None:
    """
    ARRANGE: two viable quotes share the same longname but differ in symbol
    ACT:     call _choose_symbol directly
    ASSERT:  first symbol in list is returned
    """
    viable = [
        {"symbol": "FIRST", "longname": "Same Co"},
        {"symbol": "SECOND", "longname": "Same Co"},
    ]

    actual = _choose_symbol(
        viable,
        expected_name="Same Co",
        expected_symbol="FIRST",
        min_score=150,
    )

    assert actual == "FIRST"


async def test_try_identifier_no_viable_quotes() -> None:
    """
    ARRANGE: search returns quotes missing required fields
    ACT:     call _try_identifier
    ASSERT:  raises LookupError (branch where 'No viable candidates' is raised)
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

    with pytest.raises(LookupError) as error:
        await feed._try_identifier("ID", "Name", "SYM")
    await close(session._client)

    assert "Quote Search endpoint returned nothing." in str(error.value)


async def test_try_identifier_single_viable_returns_info() -> None:
    """
    ARRANGE: identifier search yields a single viable quote
    ACT:     call _try_identifier
    ASSERT:  flattened info dict is returned
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

    actual = await feed._try_identifier("id", "Apple Inc", "AAPL")
    await close(session._client)

    assert actual["regularMarketPrice"] == expected_regular_market_price


async def test_identifier_quote_summary_missing_raises() -> None:
    """
    ARRANGE: viable quote found but quoteSummary endpoint returns None
    ACT:     call _try_identifier
    ASSERT:  raises LookupError (covers 'info is None' branch)
    """
    search_payload = {
        "quotes": [
            {"symbol": "NOSUM", "longname": "No Summary Ltd", "quoteType": "EQUITY"},
        ],
    }
    patterns = {
        "finance/search": httpx.Response(200, json=search_payload),
        "getcrumb": httpx.Response(200, text='"crumb"'),
        "quoteSummary": httpx.Response(200, json={"quoteSummary": {"result": None}}),
    }

    session = make_session(handler_factory(patterns))
    feed = YFinanceFeed(session)

    with pytest.raises(LookupError) as error:
        await feed._try_identifier("XYZ", "No Summary Ltd", "NOSUM")
    await close(session._client)

    assert "Quote Summary endpoint returned nothing." in str(error.value)


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


async def test_try_name_or_symbol_low_score() -> None:
    """
    ARRANGE: Yahoo search returns multiple mismatching quotes
    ACT:     call _try_name_or_symbol
    ASSERT:  raises LookupError
    """
    search_payload = {
        "quotes": [
            {"symbol": "BAD1", "shortname": "Wrong Co One", "quoteType": "EQUITY"},
            {"symbol": "BAD2", "shortname": "Wrong Co Two", "quoteType": "EQUITY"},
        ],
    }

    session = make_session(
        handler_factory({"finance/search": httpx.Response(200, json=search_payload)}),
    )
    feed = YFinanceFeed(session)

    with pytest.raises(LookupError) as error:
        await feed._try_name_or_symbol("Right Name", "Right Name", "RGT")
    await close(session._client)

    assert "No candidate matched" in str(error.value)


async def test_try_name_or_symbol_unviable_quotes() -> None:
    """
    ARRANGE: search returns quotes that are present but not viable (missing names)
    ACT:     call _try_name_or_symbol
    ASSERT:  raises LookupError (branch where `if not viable:` triggers 'continue')
    """
    search_payload = {
        "quotes": [
            {"symbol": "SYM", "longname": None, "shortname": None},
            {"symbol": "SYM2", "longname": "", "shortname": ""},
        ],
    }

    session = make_session(
        handler_factory({"finance/search": httpx.Response(200, json=search_payload)}),
    )
    feed = YFinanceFeed(session)

    with pytest.raises(LookupError):
        await feed._try_name_or_symbol("Some Co", "Some Co", "SYM")
    await close(session._client)

    assert True


async def test_fetch_equity_no_data_raises_and_skips_cache() -> None:
    """
    ARRANGE: Yahoo returns no viable quotes
    ACT:     call fetch_equity
    ASSERT:  raises LookupError and cache remains empty
    """
    session = make_session(  # search endpoint returns empty quotes list
        handler_factory({"finance/search": httpx.Response(200, json={"quotes": []})}),
    )
    feed = YFinanceFeed(session)

    with pytest.raises(LookupError):
        await feed.fetch_equity(symbol="MISS", name="Missing Inc")
    await close(session._client)

    assert load_cache_entry("yfinance_equities", "MISS") is None


async def test_try_identifier_multiple_viable_selects_best() -> None:
    """
    ARRANGE: identifier search yields two viable quotes
    ACT:     call _try_identifier
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

    info = await feed._try_identifier("010101", "Best Plc", "BEST")
    await close(session._client)

    assert info["metric"] == expected_metric


async def test_try_identifier_multiple_viable_none_selected() -> None:
    """
    ARRANGE: identifier search yields two viable quotes but none pass fuzzy score
    ACT:     call _try_identifier
    ASSERT:  raises LookupError
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

    with pytest.raises(LookupError) as error:
        await feed._try_identifier("ID123", "Gamma Ltd", "GMM")
    await close(session._client)

    assert "Low Fuzzy Score" in str(error.value)


async def test_try_name_or_symbol_no_quotes() -> None:
    """
    ARRANGE: search returns zero quotes
    ACT:     call _try_name_or_symbol
    ASSERT:  raises LookupError
    """
    session = make_session(
        handler_factory({"finance/search": httpx.Response(200, json={"quotes": []})}),
    )
    feed = YFinanceFeed(session)

    with pytest.raises(LookupError) as error:
        await feed._try_name_or_symbol("Nobody Co", "Nobody Co", "NONE")
    await close(session._client)

    assert "No candidate matched" in str(error.value)


async def test_fetch_equity_hits_cache_write_branch() -> None:
    """
    ARRANGE: subclass whose _try_identifier always succeeds
    ACT:     call fetch_equity
    ASSERT:  record is persisted to cache
    """

    class _AlwaysSucceedsFeed(YFinanceFeed):
        async def _try_identifier(self, *_: object, **__: object) -> dict:
            return {"symbol": "COV", "metric": 99}

        async def _try_name_or_symbol(self, *_: object, **__: object) -> None:
            raise AssertionError("Should not be reached")

    # network never reached, but a session is still required
    session = make_session(lambda _: httpx.Response(204))

    feed = _AlwaysSucceedsFeed(session)
    record = await feed.fetch_equity(symbol="COV", name="Covered Corp", isin="ID123")
    await close(session._client)

    assert load_cache_entry("yfinance_equities", "COV") == record


async def test_retrieve_first_identifier_hit_via_fetch_equity() -> None:
    """
    ARRANGE: first identifier search succeeds via overridden method
    ACT:     call fetch_equity
    ASSERT:  success returned immediately
    """

    class _FirstHitFeed(YFinanceFeed):
        async def _try_identifier(self, *_: object, **__: object) -> dict:
            return {"symbol": "WIN"}

        async def _try_name_or_symbol(self, *_: object, **__: object) -> None:
            raise AssertionError("Should not be reached")

    session = make_session(lambda _: httpx.Response(204))

    feed = _FirstHitFeed(session)
    actual = await feed.fetch_equity(symbol="WIN", name="Winner Co", isin="ISIN123")
    await close(session._client)

    assert actual == {"symbol": "WIN"}


async def test_try_name_or_symbol_match_returns_info() -> None:
    """
    ARRANGE: search returns a quote whose shortname/symbol match inputs closely
    ACT:     call _try_name_or_symbol
    ASSERT:  info dict from quoteSummary is returned
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

    actual = await feed._try_name_or_symbol("Good Co", "Good Co", "GOOD")
    await close(session._client)

    assert actual["answer"] == expected_answer


async def test_try_identifier_unviable_quotes_raises() -> None:
    """
    ARRANGE: search returns quotes that have a symbol but no long-name or short-name
    ACT:     call _try_identifier
    ASSERT:  LookupError message is 'No viable candidates found.'
    """
    search_payload = {
        "quotes": [
            {
                "symbol": "AAA",
                "longname": None,
                "shortname": None,
                "quoteType": "EQUITY",
            },
        ],
    }
    session = make_session(
        handler_factory({"finance/search": httpx.Response(200, json=search_payload)}),
    )
    feed = YFinanceFeed(session)

    with pytest.raises(LookupError) as error:
        await feed._try_identifier("AAA-ID", "Alpha Co", "AAA")
    await close(session._client)

    assert "No viable candidates found." in str(error.value)


async def test_fetch_equity_identifier_none_then_name_hit() -> None:
    """
    ARRANGE: _try_identifier returns None, _try_name_or_symbol succeeds
    ACT:     call fetch_equity
    ASSERT:  the record from _try_name_or_symbol is returned
    """

    class _PartialHitFeed(YFinanceFeed):
        async def _try_identifier(self, *_: object, **__: object) -> dict | None:
            return None

        async def _try_name_or_symbol(self, *_: object, **__: object) -> dict:
            return {"symbol": "FLB", "metric": 88}

    session = make_session(lambda _: httpx.Response(204))  # network never reached
    feed = _PartialHitFeed(session)

    excepted_metric = 88

    record = await feed.fetch_equity(symbol="FLB", name="Fallback Plc", isin="ISIN888")
    await close(session._client)

    assert record["metric"] == excepted_metric


async def test_try_name_or_symbol_continue_then_success() -> None:
    """
    ARRANGE: first search yields unviable quotes, second yields viable quote and summary
    ACT:     call _try_name_or_symbol
    ASSERT:  returns expected info dict
    """
    unviable = {
        "quotes": [
            {
                "symbol": "BAD",
                "longname": None,
                "shortname": None,
                "quoteType": "EQUITY",
            },
        ],
    }
    viable = {
        "quotes": [{"symbol": "GOOD", "shortname": "Good Plc", "quoteType": "EQUITY"}],
    }
    summary = {"quoteSummary": {"result": [{"price": {"answer": 999}}]}}

    responses = iter([unviable, viable])

    expected_answer = 999

    # Handler cycles through unviable then viable search, and returns summary
    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        response_map = {
            "finance/search": lambda: httpx.Response(200, json=next(responses)),
            "getcrumb": lambda: httpx.Response(200, text='"crumb"'),
            "quoteSummary": lambda: httpx.Response(200, json=summary),
        }
        for key, resp_func in response_map.items():
            if key in path:
                return resp_func()
        return httpx.Response(404)

    session = make_session(handler)
    info = await YFinanceFeed(session)._try_name_or_symbol(
        "Good Plc",
        "Good Plc",
        "GOOD",
    )
    await close(session._client)

    assert info["answer"] == expected_answer


async def test_identifier_quote_summary_none_raises() -> None:
    """
    ARRANGE: quoteSummary 500 ➜ fallback empty; patch sleep to no-op
    ACT:     call _try_identifier
    ASSERT:  LookupError with 'HTTP 500' is raised after retries
    """
    real_sleep = asyncio.sleep

    async def _instant(_delay: float) -> None:
        return None

    asyncio.sleep = _instant
    try:
        patterns = {
            "finance/search": httpx.Response(
                200,
                json={
                    "quotes": [
                        {
                            "symbol": "EMPTY",
                            "longname": "Empty Plc",
                            "quoteType": "EQUITY",
                        },
                    ],
                },
            ),
            "getcrumb": httpx.Response(200, text='"crumb"'),
            "quoteSummary": httpx.Response(500),
            "/v7/finance/quote": httpx.Response(
                200,
                json={"quoteResponse": {"result": []}},
            ),
        }

        session = make_session(handler_factory(patterns))
        feed = YFinanceFeed(session)

        with pytest.raises(LookupError) as exc:
            await feed._try_identifier("ID-EMPTY", "Empty Plc", "EMPTY")
        assert "HTTP 500" in str(exc.value)
    finally:
        asyncio.sleep = real_sleep
        await close(session._client)


async def test_try_identifier_info_none_raises() -> None:
    """
    ARRANGE: search returns one viable quote;
             quoteSummary responds 401 to trigger fallback;
             fallback returns empty list so info becomes None
    ACT:     call _try_identifier
    ASSERT:  LookupError with 'Quote Summary endpoint returned nothing.' is raised
    """
    search_payload = {
        "quotes": [
            {"symbol": "MISS", "longname": "Missing Plc", "quoteType": "EQUITY"},
        ],
    }

    patterns = {
        "finance/search": httpx.Response(200, json=search_payload),
        "getcrumb": httpx.Response(200, text='"crumb"'),
        "quoteSummary": httpx.Response(httpx.codes.UNAUTHORIZED),
        "/v7/finance/quote": httpx.Response(
            200,
            json={"quoteResponse": {"result": []}},
        ),
    }

    session = make_session(handler_factory(patterns))
    feed = YFinanceFeed(session)

    with pytest.raises(LookupError) as exc:
        await feed._try_identifier("ID-MISS", "Missing Plc", "MISS")

    assert "Quote Summary endpoint returned nothing." in str(exc.value)

    await close(session._client)
