# yfinance/test_session.py


import asyncio
from collections import deque

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

    url = f"{config.quote_summary_url}MSFT?modules=price"
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

    actual = session._requires_crumb(f"{config.quote_summary_url}AAPL")

    assert actual is True


async def test_get_retries_after_401_and_adds_crumb() -> None:
    """
    ARRANGE: server replies [401, 200,200,200,200(token),200(data)]
    ACT:     session.get()
    ASSERT:  session._crumb == "token"
    """
    responses = deque(
        [
            httpx.Response(401),  # first quote → retry
            httpx.Response(200),  # seed: https://fc.yahoo.com
            httpx.Response(200),  # seed: finance.yahoo.com
            httpx.Response(200),  # seed: finance.yahoo.com/quote/…
            httpx.Response(200, text='"token"'),  # crumb endpoint
            httpx.Response(200, json={"quoteSummary": {"result": []}}),  # retry quote
        ],
    )

    async def handler(request: httpx.Request) -> httpx.Response:
        return responses.popleft()

    session = YFSession(FeedConfig(), make_client(handler))

    # trigger the 401→bootstrap→retry code path
    await session.get(f"{session.config.quote_summary_url}IBM")

    assert session._crumb == "token"


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


def test_attach_crumb_inserts_when_token_available() -> None:
    """
    ARRANGE: session primed with a crumb; quote URL targeted
    ACT:     invoke _attach_crumb
    ASSERT:  crumb key is present in returned params
    """
    config = FeedConfig()
    session = YFSession(config, make_client(lambda r: httpx.Response(200)))
    session._crumb = "abc123"

    params = session._attach_crumb(
        f"{config.quote_summary_url}GOOG",
        {"modules": "price"},
    )

    assert params["crumb"] == "abc123"


async def test_bootstrap_returns_immediately_if_crumb_set() -> None:
    """
    ARRANGE: session already stores a crumb token
    ACT:     call _bootstrap_and_fetch_crumb()
    ASSERT:  no additional network calls are made
    """
    hit_counter = {"gets": 0}

    async def handler(request: httpx.Request) -> httpx.Response:
        hit_counter["gets"] += 1
        return httpx.Response(200)

    client = make_client(handler)
    session = YFSession(FeedConfig(), client)
    session._crumb = "existing"

    await session._bootstrap_and_fetch_crumb("AAPL")
    await close(session._client)

    assert hit_counter["gets"] == 0


def test_bootstrap_inner_guard_directly() -> None:
    """
    ARRANGE: session without crumb, dummy lock presets crumb on enter
    ACT:     call _bootstrap_and_fetch_crumb
    ASSERT:  returns early inside lock, never calls out
    """
    config = FeedConfig()

    def handler(request: httpx.Request) -> httpx.Response:
        raise AssertionError("Client should not be used")

    session = YFSession(config, make_client(handler))
    session._crumb = None

    # dummy lock that sets crumb during __aenter__
    from types import TracebackType

    class DummyLock:
        async def __aenter__(self) -> None:
            session._crumb = "preset"

        async def __aexit__(
            self,
            exc_type: type[BaseException] | None,
            exc: BaseException | None,
            tb: TracebackType | None,
        ) -> None:
            pass

    session._crumb_lock = DummyLock()

    # run it synchronously
    asyncio.run(session._bootstrap_and_fetch_crumb("DUMMY"))
    assert session._crumb == "preset"
