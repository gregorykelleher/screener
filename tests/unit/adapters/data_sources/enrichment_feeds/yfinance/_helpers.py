# yfinance/_helpers.py

from collections.abc import Callable, Mapping

import httpx

from equity_aggregator.adapters.data_sources.enrichment_feeds.yfinance.config import (
    FeedConfig,
)
from equity_aggregator.adapters.data_sources.enrichment_feeds.yfinance.session import (
    YFSession,
)


def make_client(
    handler: Callable[[httpx.Request], httpx.Response],
) -> httpx.AsyncClient:
    """
    Creates an asynchronous HTTPX client using a custom request handler.

    Args:
        handler (Callable[[httpx.Request], httpx.Response]): A callable that takes an
            httpx.Request and returns an httpx.Response, used to mock HTTP responses.

    Returns:
        httpx.AsyncClient: An asynchronous HTTP client configured with the provided
            mock transport handler.
    """

    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


def make_session(handler: Callable[[httpx.Request], httpx.Response]) -> YFSession:
    """
    Creates a YFSession instance with a mocked HTTPX transport for unit testing.

    Args:
        handler (Callable[[httpx.Request], httpx.Response]): A callable that takes an
            httpx.Request and returns an httpx.Response, used to mock HTTP responses.

    Returns:
        YFSession: A YFSession object configured with a mock HTTP client and default
            FeedConfig.
    """
    client = make_client(handler)
    return YFSession(FeedConfig(), client)


def handler_factory(
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
        return httpx.Response(200, json={})

    return handler


async def close(client: httpx.AsyncClient) -> None:
    """
    Asynchronously closes the provided HTTPX AsyncClient instance.

    Args:
        client (httpx.AsyncClient): The asynchronous HTTP client to close.

    Returns:
        None
    """
    await client.aclose()
