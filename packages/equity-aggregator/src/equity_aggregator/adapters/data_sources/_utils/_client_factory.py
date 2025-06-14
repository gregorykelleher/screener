# _utils/_client_factory.py

from collections.abc import Callable
from functools import partial

from httpx import AsyncClient, AsyncHTTPTransport, Limits, Timeout

ClientFactory = Callable[..., AsyncClient]


def make_client_factory(**overrides: object) -> ClientFactory:
    """
    Create a factory for httpx.AsyncClient with default settings and optional overrides.

    Args:
        **overrides: Arbitrary keyword arguments to override default AsyncClient
            parameters such as base_url, headers, timeout, etc.

    Returns:
        Callable[..., AsyncClient]: A function that creates AsyncClient instances
            with merged overrides and per-call overrides.

    Example:
        xetra_client = make_client_factory(
            base_url="https://api.xetra.de",
            headers={"X-API-Key": "..."},
        )
        async with xetra_client(timeout=10.0) as client:
            ...
    """

    limits = Limits(
        max_connections=128,
        max_keepalive_connections=64,
    )

    timeout = Timeout(
        connect=3.0,  # 3s to establish TLS
        read=None,  # no read timeout
        write=5.0,  # up to 5s to send a body
        pool=None,  # no pool timeout
    )

    transport = AsyncHTTPTransport(
        http2=True,
        retries=1,
        limits=limits,
    )

    headers = {
        # accept anything
        "Accept": "*/*",
        # accept compressed responses
        "Accept-Encoding": "gzip",
        # generic language hint
        "Accept-Language": "en-US,en;q=0.9",
        # user agent to identify the client
        "User-Agent": "Mozilla/5.0",
    }

    base_params: dict[str, object] = {
        "http2": True,
        "transport": transport,
        "timeout": timeout,
        "headers": headers,
        **overrides,
    }

    return partial(AsyncClient, **base_params)
