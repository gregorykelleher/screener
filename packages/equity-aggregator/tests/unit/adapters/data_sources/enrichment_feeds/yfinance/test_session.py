# yfinance/test_session.py

import asyncio
from collections import deque
from types import TracebackType
from typing import Never

import httpx
import pytest

from equity_aggregator.adapters.data_sources.enrichment_feeds.yfinance.config import (
    FeedConfig,
)
from equity_aggregator.adapters.data_sources.enrichment_feeds.yfinance.session import (
    YFSession,
)
from tests.unit.adapters.data_sources.enrichment_feeds.yfinance._helpers import (
    close,
    make_client,
)

pytestmark = pytest.mark.unit


def test_extract_ticker_returns_expected() -> None:
    """
    ARRANGE: typical quoteSummary URL
    ACT:     call _extract_ticker
    ASSERT:  ticker is extracted
    """

    cfg = FeedConfig()
    session = YFSession(cfg, make_client(lambda r: httpx.Response(200)))

    url = f"{cfg.quote_summary_url}MSFT?modules=price"
    ticker = session._extract_ticker(url)

    assert ticker == "MSFT"


async def test_get_retries_after_401_and_adds_crumb() -> None:
    """
    ARRANGE: server replies [401, seeds, crumb, retry]
    ACT:     session.get()
    ASSERT:  session._crumb == "token"
    """

    responses = deque(
        [
            httpx.Response(401),  # first quote
            httpx.Response(200),  # https://fc.yahoo.com
            httpx.Response(200),  # finance.yahoo.com
            httpx.Response(200),  # finance.yahoo.com/quote/…
            httpx.Response(200, text='"token"'),  # crumb endpoint
            httpx.Response(200, json={"quoteSummary": {"result": []}}),  # retry
        ],
    )

    async def handler(_request: httpx.Request) -> httpx.Response:
        return responses.popleft()

    session = YFSession(FeedConfig(), make_client(handler))

    await session.get(f"{session.config.quote_summary_url}IBM")
    await close(session._client)

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
    ARRANGE: handler that records received query parameters
    ACT:     call get() without params
    ASSERT:  handler sees an empty param dict
    """
    cfg = FeedConfig()
    captured: dict[str, object] = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        captured["params"] = dict(request.url.params)
        return httpx.Response(200)

    session = YFSession(cfg, make_client(handler))

    await session.get(cfg.search_url)
    await close(session._client)

    assert captured["params"] == {}


def test_attach_crumb_is_noop_after_token_comment_out() -> None:
    """
    ARRANGE: crumb pre-set; target URL is quote-summary
    ACT:     call _attach_crumb
    ASSERT:  returned params unchanged (crumb injection disabled in code)
    """

    cfg = FeedConfig()
    session = YFSession(cfg, make_client(lambda r: httpx.Response(200)))
    session._crumb = "should_not_be_used"

    original = {"modules": "price"}
    updated = session._attach_crumb(f"{cfg.quote_summary_url}GOOG", original.copy())

    assert updated == original


async def test_bootstrap_returns_immediately_if_crumb_set() -> None:
    """
    ARRANGE: session already possesses a crumb token
    ACT:     call _bootstrap_and_fetch_crumb()
    ASSERT:  no outbound HTTP requests occur
    """

    hit_counter = {"gets": 0}

    async def handler(_request: httpx.Request) -> httpx.Response:
        hit_counter["gets"] += 1
        return httpx.Response(200)

    session = YFSession(FeedConfig(), make_client(handler))
    session._crumb = "existing"

    await session._bootstrap_and_fetch_crumb("AAPL")
    await close(session._client)

    assert hit_counter["gets"] == 0


def test_bootstrap_inner_guard_directly() -> None:
    """
    ARRANGE: session without crumb, dummy lock sets crumb on enter
    ACT:     call _bootstrap_and_fetch_crumb synchronously
    ASSERT:  crumb updated, no network calls made
    """

    cfg = FeedConfig()

    def handler(_request: httpx.Request) -> httpx.Response:
        raise AssertionError("HTTP client should not be used")

    session = YFSession(cfg, make_client(handler))
    session._crumb = None

    class DummyLock:
        async def __aenter__(self) -> None:
            session._crumb = "preset"

        async def __aexit__(
            self,
            _exc_type: type[BaseException] | None,
            _exc: BaseException | None,
            _tb: TracebackType | None,
        ) -> None: ...

    session._crumb_lock = DummyLock()

    asyncio.run(session._bootstrap_and_fetch_crumb("DUMMY"))
    assert session._crumb == "preset"


async def test_safe_get_raises_protocol_error_after_retries() -> None:
    """
    ARRANGE: handler always triggers httpx.ProtocolError
    ACT:     call _safe_get with retries=1
    ASSERT:  ProtocolError is raised
    """

    async def boom(_request: httpx.Request) -> httpx.Response:
        raise httpx.ProtocolError("boom")

    session = YFSession(FeedConfig(), make_client(boom))

    with pytest.raises(httpx.ProtocolError):
        await session._safe_get("https://dummy", params={}, retries=1)


async def test_reset_client_clears_crumb() -> None:
    """
    ARRANGE: session with crumb preset
    ACT:     call _reset_client()
    ASSERT:  _crumb becomes None
    """

    session = YFSession(FeedConfig(), make_client(lambda r: httpx.Response(200)))
    session._crumb = "abc"

    await session._reset_client()

    assert session._crumb is None


async def test_get_with_backoff_returns_200_after_retry() -> None:
    """
    ARRANGE: transport yields 429 first, 200 next
    ACT:     session.get()
    ASSERT:  final status code is 200
    """
    replies = deque([httpx.Response(429), httpx.Response(200)])
    expected_status_code = 200

    async def handler(_req: httpx.Request) -> httpx.Response:
        return replies.popleft()

    session = YFSession(FeedConfig(), make_client(handler))

    response = await session.get("https://dummy/quoteSummary/ZZZ")

    assert response.status_code == expected_status_code


async def test_get_with_backoff_propagates_protocol_error() -> None:
    """
    ARRANGE: first response 429; _safe_get raises ProtocolError
    ACT:     call _get_with_backoff directly
    ASSERT:  ProtocolError is re-raised
    """

    class ErrorSession(YFSession):
        async def _safe_get(self, *_a: object, **_kw: object) -> Never:
            raise httpx.ProtocolError("boom")

    first429 = httpx.Response(429)
    sess = ErrorSession(FeedConfig(), make_client(lambda r: first429))

    with pytest.raises(httpx.ProtocolError):
        await sess._get_with_backoff("https://dummy", {}, first429)


async def test_get_with_backoff_returns_after_max_attempts() -> None:
    """
    ARRANGE: all five retries stay 429, sleep mocked to zero-delay
    ACT:     call _get_with_backoff directly
    ASSERT:  response with status 429 is returned after loop completes
    """

    expected_status_code = 429

    # Mock the sleep function to return immediately
    real_sleep = asyncio.sleep

    async def _instant(_delay: float) -> None:
        return None

    asyncio.sleep = _instant

    try:
        reps = deque([httpx.Response(429)] * 5)  # five retries
        first = httpx.Response(429)  # initial

        class Always429(YFSession):
            async def _safe_get(self, *_a: object, **_k: object) -> httpx.Response:
                return reps.popleft() if reps else first

        session = Always429(FeedConfig(), make_client(lambda r: first))

        final = await session._get_with_backoff("https://dummy", {}, first)

        assert final.status_code == expected_status_code

    finally:
        asyncio.sleep = real_sleep
