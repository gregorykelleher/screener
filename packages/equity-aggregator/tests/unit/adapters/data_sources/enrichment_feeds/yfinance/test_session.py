# yfinance/test_session.py


import httpx
import pytest

from equity_aggregator.adapters.data_sources.enrichment_feeds.yfinance.config import (
    FeedConfig,
)
from equity_aggregator.adapters.data_sources.enrichment_feeds.yfinance.session import (
    YFSession,
)

from ._utils import close, make_client

pytestmark = pytest.mark.unit


def test_extract_ticker_returns_expected() -> None:
    """
    ARRANGE: typical quoteSummary URL
    ACT:     call _extract_ticker
    ASSERT:  ticker is extracted
    """
    config = FeedConfig()
    session = YFSession(config, make_client(lambda r: httpx.Response(200)))

    url = f"{config.quote_base}MSFT?modules=price"
    ticker = session._extract_ticker(url)

    assert ticker == "MSFT"


def test_requires_crumb_true_only_for_quote_urls_without_token() -> None:
    """
    ARRANGE: crumb unset, URL starts with quote_base
    ACT:     call _requires_crumb
    ASSERT:  returns True
    """
    config = FeedConfig()
    session = YFSession(config, make_client(lambda r: httpx.Response(200)))

    actual = session._requires_crumb(f"{config.quote_base}AAPL")

    assert actual is True


async def test_get_bootstraps_and_injects_crumb() -> None:
    """
    ARRANGE: first call needs crumb; mock getcrumb returns "token"
    ACT:     call get(), then inspect query params of quote request
    ASSERT:  crumb param was attached
    """
    config = FeedConfig()
    recorded: dict[str, str | None] = {"crumb": None}

    async def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)

        if "getcrumb" in url:
            return httpx.Response(200, text='"token"')

        if config.quote_base in url:
            recorded["crumb"] = request.url.params.get("crumb")
            return httpx.Response(200, json={"quoteSummary": {"result": []}})

        # seed bootstrap calls
        return httpx.Response(200)

    client = make_client(handler)
    session = YFSession(config, client)

    await session.get(f"{config.quote_base}TSLA", params={"modules": "price"})
    await close(session._client)

    assert recorded["crumb"] == "token"


async def test_aclose_marks_client_closed() -> None:
    """
    ARRANGE: fresh session
    ACT:     call aclose()
    ASSERT:  client reports closed
    """
    client = make_client(lambda r: httpx.Response(200))
    session = YFSession(FeedConfig(), client)

    await close(session._client)

    assert client.is_closed


async def test_get_defaults_params_to_empty_dict() -> None:
    """
    ARRANGE:  Session with a handler that records query parameters
    ACT:      call get() without passing `params`
    ASSERT:   handler receives an empty dict (branch where params=None → {})
    """
    config = FeedConfig()
    captured: dict[str, object] = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        captured["params"] = dict(request.url.params)
        return httpx.Response(200)

    client = make_client(handler)
    session = YFSession(config, client)

    await session.get(config.search_url)
    await close(session._client)

    assert captured["params"] == {}
