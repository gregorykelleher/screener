# _utils/_client_factory.py

from collections.abc import Callable
from functools import partial

from httpx import AsyncClient, AsyncHTTPTransport, Limits, Timeout

ClientFactory = Callable[..., AsyncClient]

DEFAULT_TIMEOUT = Timeout(connect=5.0, read=30.0, write=10.0, pool=5.0)
DEFAULT_LIMITS = Limits(max_connections=100, max_keepalive_connections=20)
DEFAULT_TRANSPORT = AsyncHTTPTransport(retries=2)
DEFAULT_HEADERS: dict[str, str] = {
    # accept anything
    "Accept": "*/*",
    # accept compressed responses
    "Accept-Encoding": "gzip, deflate, br",
    # generic language hint
    "Accept-Language": "en-US,en;q=0.9",
    # user agent to identify the client
    "User-Agent": "Mozilla/5.0",
    # Keep connections alive by default
    "Connection": "keep-alive",
}


def make_client_factory(**defaults: object) -> ClientFactory:
    """
    Create a factory for httpx.AsyncClient with default settings and optional overrides.

    Args:
        **defaults: Arbitrary keyword arguments to override default AsyncClient
            parameters such as base_url, headers, timeout, etc.

    Returns:
        Callable[..., AsyncClient]: A function that creates AsyncClient instances
            with merged defaults and per-call overrides.

    Example:
        xetra_client = make_client_factory(
            base_url="https://api.xetra.de",
            headers={"X-API-Key": "..."},
        )
        async with xetra_client(timeout=10.0) as client:
            ...
    """

    base_params: dict[str, object] = {
        "timeout": DEFAULT_TIMEOUT,
        "limits": DEFAULT_LIMITS,
        "transport": DEFAULT_TRANSPORT,
        "headers": DEFAULT_HEADERS,
        **defaults,
    }

    return partial(AsyncClient, **base_params)
